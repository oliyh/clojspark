(ns spark.core
  (:require [clojure.string :as s]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as s-de])
  )




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Basic Spark management

(defn make-spark-context []
  (let [c (-> (conf/spark-conf)
              (conf/master "local")
              (conf/app-name "spark-example"))]
    (spark/spark-context c)))



(defn -main [& args]
  (let [sc (make-spark-context)
        input-rdd (spark/text-file sc "hdfs://localhost/user/clojspark/basics/inputdata")
	  words-rdd (spark/flat-map (fn [l] (s/split l #" ")) input-rdd)
	  count-rdd (->> (spark/map-to-pair (fn [w] (spark/tuple w 1)) words-rdd) (spark/reduce-by-key +) )
	  res (spark/map (s-de/key-value-fn (fn [k v] (str "(" k ", " v ")"))) count-rdd) ]
    (println (spark/collect res))))
