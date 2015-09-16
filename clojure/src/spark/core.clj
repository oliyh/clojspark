(ns spark.core
  (:require [clojure.string :as s]
            [spark.pprint :as pprint]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as s-de])
  (:gen-class))

(defn ->sale-record [line]
  (update-in (->> (s/split line #",")
                  (drop 1)
                  (map #(s/replace % #"\"" ""))
                  (zipmap [:price :date :postcode]))
             [:price] #(Integer/parseInt %)))

(defn highest-price [a b]
  (if (< (:price b) (:price a))
    a
    b))

(defn lowest-price [a b]
  (if (< (:price b) (:price a))
    b
    a))

(defn do-run [sc]
  (let [input-rdd (spark/text-file sc "hdfs://localhost/user/clojspark/basics/inputdata/house-prices.csv")
        postcode-groups (->> (spark/flat-map (fn [l] (s/split l #"\n")) input-rdd)
                             (spark/map-to-pair (fn [l]
                                                  (let [record (->sale-record l)]
                                                    (spark/tuple (first (s/split (:postcode record) #" ")) record)))))

        highest-prices (->> postcode-groups
                            (spark/reduce-by-key highest-price))

        lowest-prices (->> postcode-groups
                           (spark/reduce-by-key lowest-price))

        price-brackets (spark/join highest-prices lowest-prices)]

    (spark/take 10 price-brackets)))

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
  (pprint/spprint (do-run (make-spark-context false))))
