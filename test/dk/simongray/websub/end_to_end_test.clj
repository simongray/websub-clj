(ns dk.simongray.websub.end-to-end-test
  "A subscriber, a publisher and a hub of this library that talk over HTTP
  on localhost, with the redirects of websub.rocks's tests 201 to 204."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [dk.simongray.wary-fetch :as http]
            [dk.simongray.webcrypto.bytes :as bytes]
            [dk.simongray.websub :as websub]
            [dk.simongray.websub.hub :as hub]
            [dk.simongray.websub.test-support :refer [with-server]]))

(defn- send-without-tls!
  "Send the `request` as http/send! does, or refuse an https URL at once,
  as a host without TLS does. The test server would rather wait for an
  answer that never comes."
  [request]
  (if (str/starts-with? (:url request) "https:")
    (throw (ex-info "No TLS on localhost" {:url (:url request)}))
    (http/send! request)))

(def anywhere
  "The options of requests to localhost."
  {:allow-pred (constantly true)
   :send       send-without-tls!})

(defn- moved
  "A Ring handler that redirects with `status` to the path `path` on the
  server at the URL in the atom `base`."
  [status base path]
  (fn [_]
    {:status status :headers {"Location" (str @base path)} :body ""}))

(defn- feed
  "A Ring handler for the topic, an RSS feed whose Link header names the
  hub and the topic on the server at the URL in the atom `base`."
  [base]
  (fn [_]
    {:status  200
     :headers {"Content-Type" "application/rss+xml"
               "Link"         (websub/link-header [(str @base "/hub")]
                                                  (str @base "/feed"))}
     :body    "<rss version=\"2.0\"><channel><title>A show</title></channel></rss>"}))

(defn- run-jobs!
  "Run each job in the atom `jobs` by `opts`, the new ones too, and give
  their reports."
  [jobs opts]
  (loop [reports []]
    (if-let [job (first @jobs)]
      (do (swap! jobs (comp vec rest))
          (recur (conj reports (hub/run-job! job opts))))
      reports)))

(deftest a-subscriber-a-publisher-and-a-hub
  (let [base       (atom nil)
        jobs       (atom [])
        came       (atom [])
        secret     "a text that nobody can guess"
        hub-opts   (delay (merge anywhere
                                 {:url       (str @base "/hub")
                                  :store     (hub/memory-store)
                                  :schedule! #(swap! jobs conj %)}))
        callback   (websub/handler {:wanted-pred (fn [{:keys [topic]}] (= topic (str @base "/feed")))
                                    :deliver!    #(swap! came conj (dissoc % :request))
                                    :secret      (fn [request]
                                                   (when (= "/signed" (:uri request)) secret))})]
    (with-server {"/feed"       (feed base)
                  "/moved-feed" (moved 302 base "/feed")
                  "/old-feed"   (moved 301 base "/feed")
                  "/hub"        (fn [request] ((hub/handler @hub-opts) request))
                  "/temp-hub"   (moved 307 base "/hub")
                  "/old-hub"    (moved 308 base "/hub")
                  "/callback"   callback
                  "/signed"     callback
                  "/moving"     (moved 302 base "/callback")}
      (fn [url]
        (reset! base url)
        (let [topic     (str url "/feed")
              subscribe #(websub/call! (websub/request :subscribe %1 topic (str url %2)) anywhere)
              publish!  #(do (websub/publish! [(str url "/hub")] topic anywhere)
                             (run-jobs! jobs @hub-opts))]
          (testing "201 and 202: the discovery of a topic that redirects"
            (doseq [path ["/moved-feed" "/old-feed"]]
              (is (= {:status 200 :hubs [(str url "/hub")] :topic topic}
                      (websub/discover! (str url path) anywhere)))))
          (testing "a subscription, over http when https fails"
            (is (= {:status 202 :url (str url "/hub")} (subscribe (str url "/hub") "/callback")))
            (is (= [true] (map :verified? (run-jobs! jobs @hub-opts)))))
          (testing "203 and 204: a hub that redirects"
            (doseq [path ["/temp-hub" "/old-hub"]]
              (is (= {:status 202 :url (str url "/hub")} (subscribe (str url path) "/callback"))))
            (is (= [true true] (map :verified? (run-jobs! jobs @hub-opts)))))
          (testing "a callback that redirects fails the check"
            (subscribe (str url "/hub") "/moving")
            (is (= [[false 302]] (map (juxt :verified? :status) (run-jobs! jobs @hub-opts)))))
          (testing "the delivery of a topic that changed"
            (is (= [[:delivered 200]]
                   (map (juxt :outcome :status) (:deliveries (first (publish!))))))
            (is (= [{:content-type "application/rss+xml" :topic topic :hubs [(str url "/hub")]
                     :body "<rss version=\"2.0\"><channel><title>A show</title></channel></rss>"}]
                   (map #(update % :body bytes/decode) @came))))
          (testing "a delivery with a signature"
            (reset! came [])
            ;; a secret goes over https alone, unless the request is sent by hand
            (http/send! (merge anywhere
                               (websub/request :subscribe (str url "/hub") topic (str url "/signed") {:secret secret})
                               {:url (str url "/hub")}))
            (run-jobs! jobs @hub-opts)
            (is (= #{[:delivered 200]}
                   (set (map (juxt :outcome :status) (:deliveries (first (publish!)))))))
            (is (= 2 (count @came)) "to the subscription with the secret too"))
          (testing "an unsubscription"
            (reset! came [])
            (websub/call! (websub/request :unsubscribe (str url "/hub") topic (str url "/callback")) anywhere)
            (websub/call! (websub/request :unsubscribe (str url "/hub") topic (str url "/signed")) anywhere)
            (is (= [true true] (map :verified? (run-jobs! jobs @hub-opts))))
            (is (= [] (:deliveries (first (publish!)))))
            (is (= [] @came))))))))
