(ns clj-max-temperature
  (:gen-class)
  (:import (org.apache.hadoop.fs Path)
           (org.apache.hadoop.io IntWritable LongWritable Text)
           (org.apache.hadoop.mapreduce Job Mapper Mapper$Context Reducer Reducer$Context)
           (org.apache.hadoop.mapreduce.lib.input FileInputFormat)
           (org.apache.hadoop.mapreduce.lib.output FileOutputFormat)
           ))

(gen-class 
 :name clj_max_temperature.mapper
 :extends org.apache.hadoop.mapreduce.Mapper
 :prefix "mapper-")

(defn mapper-map [this key value #^Mapper$Context context]
  (let [line (str value)
        year (.substring line 15 19)
        quality (.substring line 92 93)
        air-temperature (Integer/valueOf 
                         (.substring line 
                                     (if (= (.charAt line 87) \+) 88 87)
                                     92))]
    (if (and (not (= air-temperature 9999))
             (.matches quality "[01459]"))
      (.write context (Text. year) (IntWritable. air-temperature)))))

(gen-class
 :name clj_max_temperature.reducer
 :extends org.apache.hadoop.mapreduce.Reducer
 :prefix "reducer-")

(defn reducer-reduce [this key #^Iterable values #^Reducer$Context context]  
  (.write context key (IntWritable. (reduce max (map #(.get %) values)))))

(defn -main [& args]
  (when (not (= (count args) 2))
    (.println System/err "args error!")
    (System/exit -1))
  (let [job (Job.)]
    (FileInputFormat/addInputPath job (Path. (nth args 0)))    
    (FileOutputFormat/setOutputPath job (Path. (nth args 1)))
    (doto job      
      (.setJarByClass (Class/forName "clj_max_temperature"))
      (.setMapperClass (Class/forName "clj_max_temperature.mapper"))
      (.setReducerClass (Class/forName "clj_max_temperature.reducer"))
      (.setOutputKeyClass Text)
      (.setOutputValueClass IntWritable))
    (System/exit (if (.waitForCompletion job true) 0 1))))
