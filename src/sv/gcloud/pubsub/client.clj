(ns sv.gcloud.pubsub.client
  (:import org.apache.commons.codec.binary.Base64))

(defn base64-encode [string]
  (Base64/encodeBase64String
   (.getBytes string "UTF-8")))

(defn base64-decode [string]
  (String.
   (Base64/decodeBase64
    string)
   "UTF-8"))

(defn decode-message
  ([decode-fn message]
   (update-in message [:message :data] decode-fn))
  ([message]
   (decode-message base64-decode message)))

(defn decode-messages
  ([decode-fn pull-response]
   (update-in
    pull-response
    [:body :receivedMessages]
    (fn [messages]
      (map
       (partial decode-message decode-fn)
       messages))))
  ([pull-response]
   (decode-messages base64-decode pull-response)))

(defn publish
  [params]
  {:method :post
   :url (format
         "https://pubsub.googleapis.com/v1/projects/%s/topics/%s:publish"
         (:project params) (:topic params))
   :form-params
   {:messages
    (map
     #(update-in % [:data] base64-encode)
     (:messages params))}
   :content-type :json
   :as :json})

(defn ack
  [params]
  {:method :post
   :url (format
         "https://pubsub.googleapis.com/v1/projects/%s/subscriptions/%s:acknowledge"
         (:project params) (:subscription params))
   :form-params
   {:ackIds (:ackIds params)}
   :content-type :json
   :as :json})

(defn pull
  [params]
  {:method :post
   :url (format
         "https://pubsub.googleapis.com/v1/projects/%s/subscriptions/%s:pull"
         (:project params) (:subscription params))
   :form-params
   (merge
    {:returnImmediately true
     :maxMessages 1}
    (select-keys params [:returnImmediately :maxMessages]))
   :content-type :json
   :as :json})

(defn create-subscription
  [params]
  {:method :put
   :url (format
         "https://pubsub.googleapis.com/v1/projects/%s/subscriptions/%s"
         (:project params) (:subscription params))
   :form-params
   (-> params
       (assoc :topic (format
                      "projects/%s/topics/%s"
                      (or (:topic-project params)
                          (:project params))
                      (:topic params)))
       (dissoc :project :subscription))
   :content-type :json
   :as :json})

(defn delete-subscription
  [params]
  {:method :delete
   :url (format
         "https://pubsub.googleapis.com/v1/projects/%s/subscriptions/%s"
         (:project params) (:subscription params))
   :content-type :json
   :as :json})

(defn list-topics
  [params]
  {:method :get
   :url (format
         "https://pubsub.googleapis.com/v1/projects/%s/topics"
         (:project params))
   :content-type :json
   :as :json})

(defn modify-ack-deadline
  [params]
  {:method :post
   :url (format
         "https://pubsub.googleapis.com/v1/projects/%s/subscriptions/%s:modifyAckDeadline"
         (:project params) (:subscription params))
   :form-params
   (select-keys params [:ackIds :ackDeadlineSeconds])
   :content-type :json
   :as :json})
