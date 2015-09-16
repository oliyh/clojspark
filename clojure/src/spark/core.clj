(ns spark.core
  (:require [clojure.string :as s]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as s-de])
  (:gen-class))

(defn ->sale-record [line]
  (zipmap [:price :date :postcode]
          (map #(s/replace % #"\"" "") (drop 1 (s/split line #",")))))

(defn do-run [sc]
  (let [input-rdd (spark/text-file sc "hdfs://localhost/user/clojspark/basics/inputdata/house-prices.csv")
        postcode-groups (->> (spark/flat-map (fn [l] (s/split l #"\n")) input-rdd)
                             (spark/map-to-pair (fn [l]
                                                  (let [record (->sale-record l)]
                                                    (spark/tuple (first (s/split (:postcode record) #" ")) record))))
                             (spark/group-by-key))]

    (println (spark/take 10 postcode-groups))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Basic Spark management

(defn make-spark-context [local?]
  (let [c (if local?
            (-> (conf/spark-conf)
                (conf/master "local")
                (conf/app-name "Clojure Spark example"))
            (-> (conf/spark-conf)
                (conf/app-name "Clojure Spark example")))]
    (spark/spark-context c)))

(defonce local-sc (memoize (partial make-spark-context true)))

(defn run-local []
  (let [sc (local-sc)]
    (do-run sc)))

(defn -main [& args]
  (do-run (make-spark-context false)))


(comment                              )

        (comment lowest-prices (->> outcode-prices
                                    (spark/reduce-by-key min)))


(comment highest-prices (->> outcode-prices
                                     (spark/reduce-by-key max)))

(comment outcode-prices (->> postcode-groups
                                     (spark/map-to-pair (fn [t]
                                                          (println (:price (._2 t)))
                                                          (spark/tuple (._1 t) (:price (._2 t)))))))

(comment count-rdd (->> (spark/map-to-pair (fn [w] (spark/tuple w 1)) words-rdd)
                       (spark/reduce-by-key +) )

         res (spark/map (s-de/key-value-fn (fn [k v] (str "(" k ", " v ")"))) output) )
