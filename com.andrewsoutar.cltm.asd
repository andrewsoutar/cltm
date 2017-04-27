#+named-readtables (named-readtables:in-readtable :standard)

(asdf:defsystem :com.andrewsoutar.cltm
  :version "0.0.1"
  :description "Transactional Memory for Common Lisp"
  :author ("Andrew Soutar <andrew@andrewsoutar.com>")
  :maintainer "Andrew Soutar <andrew@andrewsoutar.com>"
  :class :package-inferred-system
  :depends-on (:uiop :com.andrewsoutar.cltm/cltm))
