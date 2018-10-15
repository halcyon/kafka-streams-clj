(ns com.zeddworks.streams
  (:import [org.apache.kafka.streams KafkaStreams StreamsConfig StreamsBuilder]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams.kstream ValueMapper]))

(def config
  {StreamsConfig/APPLICATION_ID_CONFIG "my-stream-processing-application"
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "10.0.0.120:31090"
   StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (class (Serdes/String))
   StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (class (Serdes/String))})

(defn stream
  [{:keys [in out xform]}]
  (let [builder (StreamsBuilder.)
        _ (.. builder
              (stream in)
              (mapValues xform)
              (to out))]
    (.start (KafkaStreams. (.build builder)
                           (StreamsConfig. config)))))

(defn -main [& args]
  (prn "starting")
  (stream {:in ["my-input-topic"]
           :out "my-output-topic"
           :xform (reify ValueMapper
                    (apply [this v]
                      ((comp str count) v)))})
  (Thread/sleep (* 60000 10))
  (prn "stopping"))
