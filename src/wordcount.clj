(ns wordcount
    (:require [clojure-hadoop.wrap :as wrap]
              [clojure-hadoop.defjob :as defjob]
              [clojure-hadoop.imports :as imp])
    (:use clojure.contrib.seq-utils)
    (:require [clojure.contrib.str-utils2 :as str2])
    (:import (java.util StringTokenizer)
	     (org.apache.hadoop.io.Text)
	     (org.apache.hadoop.io.IntWritable)
	     (org.apache.hadoop.mapreduce.ReduceContext)
))



(def delimiters "0123456789[ \t\n\r!\"#$%&'()*+,./:;<=>?@\\^`{|}~-]+")

(defn gen-n-grams [#^String s #^Integer n]
  (when (> (.length s) 0)
      (let [str (str " " s (String. ) (str2/repeat " " (- n 1)))]
        (reduce (fn [val elem]
                  (conj val (.substring str elem (+ elem n))))
                []
                (range 0 (+ 1 (.length s)))))))

(defn my-map [key #^String value]
  (map (fn [token] [token 1])
       (flatten (map #(gen-n-grams %1 3)
                     (enumeration-seq (StringTokenizer. value delimiters))))))

(defn my-reduce [key values-fn]
  [[key (reduce + (values-fn))]])

(defn string-long-writer [#^org.apache.hadoop.mapreduce.ReduceContext output
                          #^String key value]
  (.write output (org.apache.hadoop.io.Text. key) (org.apache.hadoop.io.IntWritable. value)))

(defn string-long-reduce-reader [#^org.apache.hadoop.io.Text key wvalues]
  [(.toString key)
   (fn [] (map (fn [#^org.apache.hadoop.io.IntWritable v] (.get v))
               (seq wvalues)))])

(defjob/defjob job
  :map my-map
  :map-reader wrap/int-string-map-reader
  :map-writer string-long-writer
  :reduce my-reduce
  :reduce-reader string-long-reduce-reader
  :reduce-writer string-long-writer
  :output-key org.apache.hadoop.io.Text
  :output-value org.apache.hadoop.io.IntWritable
  :input-format :text
  :output-format :text
  :compress-output false)