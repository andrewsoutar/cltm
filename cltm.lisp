#+named-readtables (named-readtables:in-readtable :standard)

(uiop:define-package :com.andrewsoutar.cltm
    (:documentation "Transactional Memory for Common Lisp")
  (:nicknames :com.andrewsoutar.cltm/cltm)
  (:use :cl)
  (:use :com.andrewsoutar.brace-lambda :named-readtables)
  (:use :sb-mop :sb-thread)
  (:export #:atomic-object #:atomic-class))
(in-package :com.andrewsoutar.cltm)

(in-readtable brace-lambda)

(defclass atomic-object ()
  ((generation :initform 0 :accessor generation)
   (epoch :initform (cons nil nil) :accessor epoch)
   (lock :initform (make-mutex) :reader lock)
   (propagating-now :initform nil :accessor propagating-now)))

(defclass atomic-class (standard-class)
  ()
  (:default-initargs :direct-superclasses `(,(find-class 'atomic-object))))

(macrolet ((defvalidator (class superclass)
             `(defmethod validate-superclass ((class ,class) (superclass ,superclass))
                (and (eq (type-of class) ',class) (eq (type-of superclass) ',superclass)))))
  (defvalidator atomic-class standard-class)
  (defvalidator atomic-class atomic-class))

(defclass atomic-slot-definition ()
  ((atomic :initarg :atomic :initform t :reader atomic)))
(defclass atomic-direct-slot-definition (atomic-slot-definition standard-direct-slot-definition) ())
(defclass atomic-effective-slot-definition (atomic-slot-definition standard-effective-slot-definition) ())

(defmethod direct-slot-definition-class ((class atomic-class) &rest initargs)
  (declare (ignore initargs))
  (find-class 'atomic-direct-slot-definition))

(defmethod effective-slot-definition-class ((class atomic-class) &rest initargs)
  (declare (ignore initargs))
  (find-class 'atomic-effective-slot-definition))

(defmethod compute-effective-slot-definition :around ((class atomic-class) name
                                                      direct-slot-definitions)
  (let (atomic-found non-atomic-found)
    (dolist (def direct-slot-definitions)
      (if (and (typep def 'atomic-slot-definition) (atomic def))
          (setf atomic-found t)
          (setf non-atomic-found t))
      (when (and atomic-found non-atomic-found)
        (error "Slot ~S has both atomic and non-atomic definitions" name)))
    (let ((effective-slotd (call-next-method)))
      (setf (slot-value effective-slotd 'atomic) atomic-found)
      effective-slotd)))

(defmethod allocate-instance :around ((class atomic-class) &rest initargs)
  (declare (ignore initargs))
  (assert (subtypep class 'atomic-object))
  (let ((instance (call-next-method)))
    (map () {when (and (typep %1 'atomic-slot-definition) (atomic %1))
              (setf (standard-instance-access instance (slot-definition-location %1))
                    (cons '..slot-unbound.. '..slot-unbound..))}
         (compute-slots class))
    instance))

(defstruct atomic-object-transaction-state
  (object (error "object") :type t)
  (writing nil :type boolean)
  (generation (error "generation") :type (and unsigned-byte fixnum))
  (epoch (error "epoch") :type (cons null null)))

(defvar *atomic-transaction-state*)

(defun get-raw-slot-value (object slot &optional writing)
  (let ((object-state
          (when (boundp '*atomic-transaction-state*)
            (let* ((ats *atomic-transaction-state*)
                   (object-state (find object ats
                                       :key #'atomic-object-transaction-state-object
                                       :test #'eq)))
              (cond ((null object-state)
                     (setf *atomic-transaction-state*
                           (cons
                            (setf object-state
                                  (make-atomic-object-transaction-state :object object
                                                                        :writing writing
                                                                        :generation (generation object)
                                                                        :epoch (epoch object)))
                            ats)))
                    ((and writing (not (atomic-object-transaction-state-writing object-state)))
                     (let (were-good (mutex (lock object)))
                       (sb-sys:without-interrupts
                         (unwind-protect
                              (progn
                                (sb-sys:allow-with-interrupts (grab-mutex mutex))
                                (sb-sys:with-local-interrupts
                                  (setf were-good (and (eql (generation object)
                                                            (atomic-object-transaction-state-generation
                                                             object-state))
                                                       (eql (epoch object)
                                                            (atomic-object-transaction-state-epoch
                                                             object-state))))
                                  (when were-good
                                    (setf (atomic-object-transaction-state-writing object-state) t))))
                           (unless were-good
                             (release-mutex mutex)
                             (throw 'retry-transaction (values))))))))
              object-state))))
    (let ((sia (standard-instance-access object (slot-definition-location slot))))
      (if (and object-state (atomic-object-transaction-state-writing object-state))
          (if writing sia (car sia))
          (cdr sia)))))

(defvar *transaction-propagation-lock* (make-mutex))
(defvar *transaction-propagators* ())

;;; FIXME HOLY SHIT THIS CODE IS UGLY
(defun call-atomically (thunk)
  (if (boundp '*atomic-transaction-state*)
      (funcall thunk)
      (loop
        (catch 'retry-transaction
          (return-from call-atomically
            (let ((*atomic-transaction-state* ())
                  completed)
              (unwind-protect
                   (multiple-value-prog1 (funcall thunk)
                     ;; FIXME right now, we accept errors even if the data was
                     ;; inconsistent. I think this is technically OK, but I
                     ;; would like to change this behavior.
                     (let* ((written-objects
                              (loop for object-state in *atomic-transaction-state*
                                    if (atomic-object-transaction-state-writing object-state)
                                      collect (atomic-object-transaction-state-object object-state)
                                    else
                                      do (when (let ((object (atomic-object-transaction-state-object object-state)))
                                                 (or (propagating-now object)
                                                     (not (eql (generation object)
                                                               (atomic-object-transaction-state-generation object-state)))
                                                     (not (eql (epoch object)
                                                               (atomic-object-transaction-state-epoch object-state)))))
                                           (throw 'retry-transaction (values)))))
                            ;; Whew! The transaction was successful.
                            (changeset
                              (loop with slot-updates
                                    for object in written-objects
                                    do (setf (propagating-now object) t
                                             slot-updates
                                             (mapcan {when (and (typep %1 'atomic-slot-definition)
                                                                (atomic %1))
                                                       (let ((raw (standard-instance-access
                                                                   object
                                                                   (slot-definition-location %1))))
                                                         (unless (eq (car raw) (cdr raw))
                                                           (setf (cdr raw) (car raw))
                                                           `((,(slot-definition-name %1) . ,(car raw)))))}
                                                     (compute-slots (class-of object))))
                                    when slot-updates
                                      collect `(,object . ,slot-updates)
                                      and do (when (zerop (incf (generation object)))
                                               (setf (epoch object) (cons nil nil)))
                                    do (setf (propagating-now object) nil))))
                       (with-mutex (*transaction-propagation-lock*)
                         (map () {let ((lock (lock %1)))
                                   (sb-sys:without-interrupts
                                     (sb-sys:allow-with-interrupts
                                       (release-mutex lock)))}
                              written-objects)
                         (map () {funcall %1 changeset} *transaction-propagators*)))
                     (setf completed t))
                (unless completed
                  (dolist (object-state *atomic-transaction-state*)
                    (when (atomic-object-transaction-state-writing object-state)
                      (let ((lock (lock (atomic-object-transaction-state-object object-state))))
                        (sb-sys:without-interrupts
                          (sb-sys:allow-with-interrupts
                            (release-mutex lock))))))))))))))

(defmacro atomically (&body body)
  `(call-atomically (lambda () ,@body)))

(defmethod slot-boundp-using-class ((class atomic-class) (object atomic-object)
                                    (slotd atomic-effective-slot-definition))
  (if (atomic slotd)
      (atomically (not (eq '..slot-unbound.. (get-raw-slot-value object slotd))))
      (call-next-method)))

(defmethod slot-makunbound-using-class ((class atomic-class) (object atomic-object)
                                        (slotd atomic-effective-slot-definition))
  (if (atomic slotd)
      (atomically (setf (car (get-raw-slot-value object slotd t)) '..slot-unbound..))
      (call-next-method)))

(defmethod slot-value-using-class ((class atomic-class) (object atomic-object)
                                   (slotd atomic-effective-slot-definition))
  (if (atomic slotd)
      (let ((raw (atomically (get-raw-slot-value object slotd))))
        (if (eq raw '..slot-unbound..)
            (slot-unbound class object (slot-definition-name slotd))
            raw))
      (call-next-method)))

(defmethod (setf slot-value-using-class) (new-value (class atomic-class) (object atomic-object)
                                          (slotd atomic-effective-slot-definition))
  (if (atomic slotd)
      (atomically (setf (car (get-raw-slot-value object slotd t)) new-value))
      (call-next-method)))
