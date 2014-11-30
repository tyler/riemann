(ns riemann.transport.ganglia
  (:import (org.jboss.netty.channel MessageEvent)
           [org.jboss.netty.handler.codec.oneone OneToOneDecoder])
  (:use [riemann.core :only [stream!]]
        [riemann.transport.udp :only [udp-server
                                      gen-udp-handler]]
        [riemann.transport :only [channel-pipeline-factory
                                  channel-group
                                  shared-execution-handler]]
        [riemann.time :only [unix-time]]
        [gloss.core :only [enum header string compile-frame ordered-map repeated]]
        [gloss.io :only [decode]]))

(def valid-types
  (set ["string" "uint16" "int16" "uint32" "int32" "float" "double"]))

(def slope-map
  { :zero 0, :positive 1, :negative 2, :both 3, :unspecified 4 })

(def gmetric-codec
  "A gloss-based codec for decoding gmetric-formatted packets.

   Gmetric packets are encoded in a binary format using an XDR-like scheme.
   Enums and bools are encoded as signed big-endian 32-bit integers. Strings
   are prefixed with an unsigned big-endian 32-bit integer and suffixed with up
   to three null-bytes to force a four-byte alignment.

   There are two types of gmetric packets (identified by the first four bytes
   of the packet), metadata and datapoints. See the metadata-frame and
   datapoint-frame codecs below for help on what is included in each type.

   This could also be used to encode gmetric-formatted packets, however we don't
   use it for such."
  (let [xdr-enum (fn [mapping] (enum :int32 mapping))
        xdr-bool (xdr-enum {false 0, true 1})
        xdr-uint :uint32
        xdr-string (header :uint32
                           (fn [n]
                             (string :ascii
                                     :length n, 
                                     :suffix (apply str 
                                                    (repeat 
                                                     (mod (- 4 (mod n 4)) 4) "\000"))))
                           (fn [body] body)) ; TODO: fake :(
        metadata-frame (compile-frame
                        (ordered-map :packet-type :metadata
                                     :host xdr-string
                                     :service xdr-string
                                     :spoofed xdr-bool
                                     :type xdr-string
                                     :metricname xdr-string
                                     :units xdr-string
                                     :slope (xdr-enum slope-map)
                                     :tmax xdr-uint
                                     :dmax xdr-uint
                                     :extras (repeated [xdr-string xdr-string]
                                                       :prefix :int32)))
        datapoint-frame (compile-frame
                         (ordered-map :packet-type :datapoint
                                      :host xdr-string
                                      :service xdr-string
                                      :spoofed xdr-bool
                                      :format-string xdr-string
                                      :metric xdr-string)
                         (fn [n] n)
                         (fn [msg] (if (= (:packet-type msg) :datapoint)
                                     (assoc msg
                                       :metric (Float. (:metric msg))
                                       :time (unix-time)))))
        gmetric-frame (compile-frame
                       (header
                        (xdr-enum {:metadata 128, :datapoint 133})
                        (fn [type]
                          (cond
                           (= type :metadata)  metadata-frame
                           (= type :datapoint) datapoint-frame))
                        (fn [body] body) ; TODO: doesn't matter, but this is not real
                        ))]
  
    gmetric-frame))

(defn decode-gmetric-packet
  "Decode a gmetric-formatted packet.

   By default, we'll extract these fields from Ganglia data packets:

   * The host name
   * The service name (or metric name in Ganglia's terms)
   * The metric value
   * Whether or not the host name is spoofed
   * The format string (yeah, I don't know either)

   Ganglia sends metrics as two separate packets. One includes all the metadata.
   While the other contains minimal metadata and the actual metric value. By
   default we don't use anything from the metadata packet. However, if provided,
   we'll pass both packet types to the parser-fn function. Using that, one could
   keep a cache of the metadata for different metrics and use that to decorate
   the incoming data packets."
  [packet parser-fn]
  (try
    (let [buffer (.toByteBuffer packet)
          res (decode gmetric-codec buffer)]
      (if parser-fn
        (merge res (parser-fn res))
        res))
    (catch Exception e {:ok :true :service "exception"})))


(defn ganglia-frame-decoder
  "Returns a OneToOneDecoder which uses decode-gmetric-packet."
  [parser-fn]
  (proxy [OneToOneDecoder] []
    (decode [context channel message]
      (decode-gmetric-packet message parser-fn))))

(defn ganglia-handler
  "Given a core and a MessageEvent, applies the message to core.
   Throws away metadata packets."
  [core stats ^MessageEvent e]
  (let [msg (.getMessage e)]
    (if (= (:packet-type msg) :datapoint)
      (stream! core msg))))

(defn ganglia-server
  "Start a ganglia-server.

  :host       \"127.0.0.1\"
  :port       2005
  :parser-fn  an optional function given to decode-gmetric-packet"
  ([] (ganglia-server {}))
  ([opts]
     (let [core (get opts :core (atom nil))
           host (get opts :host "127.0.0.1")
           port (get opts :port 2005)
           protocol (get opts :protocol :tcp)
           server udp-server
           channel-group (channel-group (str "ganglia server " host ":" port))
           ganglia-message-handler (gen-udp-handler
                                      core nil channel-group ganglia-handler)
           pipeline-factory (channel-pipeline-factory
                              ganglia-decoder (ganglia-frame-decoder (:parser-fn opts))
                              ^:shared handler ganglia-message-handler)]
       (server (merge opts
                          {:host host
                           :port port
                           :core core
                           :channel-group channel-group
                           :pipeline-factory pipeline-factory})))))
