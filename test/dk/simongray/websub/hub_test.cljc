(ns dk.simongray.websub.hub-test
  "The hub: the requests it takes, its checks of intent, the leases, and
  the deliveries with their signatures and retries. A case of
  websub.rocks's hub tests is named by its number."
  (:require #?(:cljs [dk.simongray.websub.test-setup])
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [dk.simongray.wary-fetch.async :as async]
            [dk.simongray.wary-fetch.bytes :as bytes]
            [dk.simongray.wary-fetch.secret :as secret]
            [dk.simongray.wary-fetch.url :as url]
            [dk.simongray.websub :as websub]
            [dk.simongray.websub.hub :as hub]
            [dk.simongray.websub.test-support :refer [run]]))

(def hub-url "https://hub.example.org/")
(def topic "https://example.org/feed")
(def callback "https://app.example.net/websub/4f1c9e2a?id=7")
(def t0 #inst "2026-10-10T12:00:00Z")

(defn- form
  "The Ring request that posts the form of `fields` to the hub."
  [fields]
  {:request-method :post
   :headers        {"content-type" "application/x-www-form-urlencoded"}
   :body           (url/query-string fields)})

(defn- query
  "The query parameters of the request map `r`."
  [r]
  (url/query-params (second (str/split (:url r) #"\?" 2))))

(defn- subscriber
  "A function that sends a request map of the hub, keeps it in the atom
  `sent`, and answers as a subscriber that wants every check would, with
  the `status` of each delivery, 202 by default."
  ([sent]
   (subscriber sent (constantly 202)))
  ([sent status-fn]
   (fn [request]
     (swap! sent conj request)
     (merge {:url (:url request) :headers {}}
            (if (= :post (:method request))
              {:status (status-fn request) :body ""}
              {:status 200 :body (get (query request) "hub.challenge")})))))

(defn- options
  "The options of a hub with a store in the atom `subscriptions`, whose
  requests go to `send` and whose jobs go to the atom `jobs`."
  [subscriptions send jobs]
  {:url       hub-url
   :store     (hub/memory-store subscriptions)
   :schedule! #(swap! jobs conj %)
   :send      send
   :now-fn    (constantly t0)})

(deftest the-requests-it-takes
  (let [jobs  (atom [])
        serve (hub/handler (options (atom {}) identity jobs))]
    (testing "100: a typical subscription"
      (is (= {:status 202 :headers {"Content-Type" "text/plain; charset=utf-8" "X-Content-Type-Options" "nosniff"}
              :body   ""}
             (serve (form {"hub.mode" "subscribe" "hub.topic" topic "hub.callback" callback}))))
      (is (= [{:job :verify :mode :subscribe :topic topic :callback callback}] @jobs)
          "the check, for later"))
    (testing "101 and 102: with a secret, a lease and more fields"
      (reset! jobs [])
      (is (= 202 (:status (serve (form {"hub.mode"          "subscribe"
                                        "hub.topic"         topic
                                        "hub.callback"      callback
                                        "hub.lease_seconds" 600
                                        "hub.secret"        "s3cr3t"
                                        "hub.foo"           "bar"
                                        "foo"               "bar"})))))
      (is (= [{:job :verify :mode :subscribe :topic topic :callback callback :lease-seconds 600
               :secret (secret/hide "s3cr3t")}]
             @jobs))
      (is (not (str/includes? (pr-str @jobs) "s3cr3t")) "a job that is printed doesn't show the secret"))
    (testing "104: an unsubscription"
      (reset! jobs [])
      (serve (form {"hub.mode" "unsubscribe" "hub.topic" topic "hub.callback" callback "hub.lease_seconds" "x"}))
      (is (= [{:job :verify :mode :unsubscribe :topic topic :callback callback}] @jobs)
          "whose lease is ignored"))
    (testing "a publisher that tells of a change"
      (reset! jobs [])
      (is (= 202 (:status (serve (form [["hub.mode" "publish"] ["hub.url" topic] ["hub.url" "https://example.org/b"]])))))
      (is (= 202 (:status (serve (form {"hub.mode" "publish" "hub.topic" topic})))))
      (is (= [{:job :distribute :topic topic} {:job :distribute :topic "https://example.org/b"}
              {:job :distribute :topic topic}]
             @jobs)
          "by hub.url, as many as it names, or by hub.topic"))
    (testing "the URLs, with the characters that need no escape decoded"
      (reset! jobs [])
      (serve (form {"hub.mode" "subscribe" "hub.topic" "https://example.org/%7Eme/feed%2F1" "hub.callback" callback}))
      (is (= "https://example.org/~me/feed%2F1" (:topic (first @jobs)))))
    (testing "a request that's wrong"
      (reset! jobs [])
      (doseq [[fields reason] [[{"hub.mode" "subscribe" "hub.topic" topic} "hub.callback must be an http or https URL"]
                               [{"hub.mode" "subscribe" "hub.callback" callback "hub.topic" "ftp://example.org/"}
                                "hub.topic must be an http or https URL"]
                               [{"hub.mode" "subscribe" "hub.callback" callback "hub.topic" topic "hub.lease_seconds" "-1"}
                                "hub.lease_seconds must be a positive integer"]
                               [{"hub.mode" "subscribe" "hub.callback" callback "hub.topic" topic
                                 "hub.secret" (apply str (repeat 200 "a"))}
                                "hub.secret is not one that this hub takes"]
                               [{"hub.mode" "publish"} "hub.url must name the topic that changed"]
                               [{"hub.mode" "renew"} "hub.mode must be subscribe, unsubscribe or publish"]]]
        (is (= [400 reason] ((juxt :status :body) (serve (form fields)))) reason))
      (is (= [] @jobs)))
    (testing "a request that the hub refuses"
      (let [picky (hub/handler (assoc (options (atom {}) identity jobs)
                                      :accept-pred (fn [job _] (str/starts-with? (:topic job) "https://example.org/"))))]
        (is (= 403 (:status (picky (form {"hub.mode" "publish" "hub.url" "https://elsewhere.example/"})))))
        (is (= 202 (:status (picky (form {"hub.mode" "publish" "hub.url" topic})))))))
    (testing "a hub with rules of its own"
      (reset! jobs [])
      (let [own (hub/handler (assoc (options (atom {}) identity jobs)
                                    :secret-pred  string?
                                    :url-fn       identity
                                    :topic-fields ["hub.topic"]))
            small (hub/handler (assoc (options (atom {}) identity jobs) :max-form-bytes 100))]
        (is (= 202 (:status (own (form {"hub.mode"     "subscribe"
                                        "hub.topic"    "https://example.org/%7Eme"
                                        "hub.callback" callback
                                        "hub.secret"   (apply str (repeat 50 "a"))})))))
        (is (= ["https://example.org/%7Eme" 50] ((juxt :topic (comp count secret/reveal :secret)) (first @jobs)))
            "a longer secret by :secret-pred, and URLs as they are by :url-fn")
        (is (= [400 "hub.topic must name the topic that changed"]
               ((juxt :status :body) (own (form {"hub.mode" "publish" "hub.url" topic}))))
            "the topics of a publisher in the :topic-fields")
        (is (= 413 (:status (small (form {"hub.mode" "publish" "hub.url" (apply str topic (repeat 100 "x"))}))))
            "and a form no larger than :max-form-bytes")))
    (is (= 202 (:status (serve {:request-method :post :form-params {"hub.mode" "publish" "hub.url" topic}})))
        "a form that Ring's middleware has read")
    (is (= [405 "POST"] ((juxt :status #(get-in % [:headers "Allow"])) (serve {:request-method :get}))))
    (is (= [308 "https://new.example.org/"]
           ((juxt :status #(get-in % [:headers "Location"]))
            ((hub/handler (assoc (options (atom {}) identity jobs) :moved-to "https://new.example.org/"))
             (form {"hub.mode" "publish" "hub.url" topic}))))
        "a hub that has moved")
    (is (thrown? #?(:clj Exception :cljs js/Error) (hub/handler {:url hub-url})) "a hub needs a store")))

(deftest checks-of-intent
  (let [subscriptions (atom {})
        sent          (atom [])
        jobs          (atom [])
        opts          (options subscriptions (subscriber sent) jobs)
        job           {:job :verify :mode :subscribe :topic topic :callback callback :lease-seconds 600
                       :secret (secret/hide "s3cr3t")}
        saved         #(get-in @subscriptions [topic callback])]
    (run
     #(async/then (hub/run-job! job opts)
                  (fn [report]
                    (let [check (peek @sent)]
                      (is (= [:get 0] [(:method check) (:max-redirects check)]) "a GET that follows no redirect")
                      (is (str/starts-with? (:url check) (str callback "&hub.mode=subscribe"))
                          "the callback with its query, and the hub's after it")
                      (is (= {"id" "7" "hub.mode" "subscribe" "hub.topic" topic "hub.lease_seconds" "3600"}
                             (dissoc (query check) "hub.challenge"))
                          "a lease no shorter than an hour")
                      (is (re-matches #"[A-Za-z0-9_-]{43}" (get (query check) "hub.challenge"))))
                    (is (= {:verified? true :status 200 :lease-seconds 3600} (select-keys report [:verified? :status :lease-seconds])))
                    (is (= {:topic topic :callback callback :secret (secret/hide "s3cr3t") :lease-seconds 3600
                            :expires #inst "2026-10-10T13:00:00Z"}
                           (saved)))))
     #(async/then (hub/run-job! (assoc job :lease-seconds 9999999 :secret nil) opts)
                  (fn [_]
                    (is (= [864000 nil] ((juxt :lease-seconds :secret) (saved)))
                        "103: a subscription again, with a lease of ten days at most and no secret now")))
     #(async/then (hub/run-job! (assoc job :lease-seconds 7200)
                                (assoc opts :send (fn [r] {:status 200 :url (:url r) :body "not the challenge"})))
                  (fn [report]
                    (is (false? (:verified? report)))
                    (is (= 864000 (:lease-seconds (saved))) "a check that fails changes nothing")))
     #(async/then (hub/run-job! (assoc job :lease-seconds 7200)
                                (assoc opts :send (fn [r] {:status 302 :url (:url r) :headers {"location" ["/x"]}})))
                  (fn [report]
                    (is (= [false 302] ((juxt :verified? :status) report)) "nor does a redirect")))
     #(async/then (hub/run-job! (assoc job :lease-seconds 7200)
                                (assoc opts :send (fn [_] (throw (ex-info "Down" {:type ::down})))))
                  (fn [report]
                    (is (= [false ::down] [(:verified? report) (get-in report [:error :type])]) "nor a subscriber that's down")))
     #(async/then (hub/run-job! (assoc job :lease-seconds 7200)
                                (assoc opts
                                       :challenge-fn       (constantly "abc")
                                       :verified-pred      (fn [challenge {:keys [body]}]
                                                             (= challenge (str/trim body)))
                                       :callback-redirects 2
                                       :send               (fn [r]
                                                             (swap! sent conj r)
                                                             {:status 200 :url (:url r) :body "abc\n"})))
                  (fn [report]
                    (is (= ["abc" 2] [(get (query (peek @sent)) "hub.challenge") (:max-redirects (peek @sent))])
                        "a challenge of :challenge-fn, and redirects by :callback-redirects")
                    (is (:verified? report) "and a check that passes by :verified-pred")
                    (is (= 7200 (:lease-seconds (saved))))))
     #(async/then (hub/run-job! {:job :verify :mode :unsubscribe :topic topic :callback callback} opts)
                  (fn [report]
                    (is (:verified? report))
                    (is (not (contains? (query (peek @sent)) "hub.lease_seconds")))
                    (is (= {} @subscriptions) "104: an unsubscription"))))))

(deftest denials
  (let [subscriptions (atom {})
        sent          (atom [])
        opts          (assoc (options subscriptions (subscriber sent) (atom []))
                             :denial-fn #(when (str/includes? (:topic %) "private") "The topic is private."))]
    (run
     #(async/then (hub/run-job! {:job :verify :mode :subscribe :topic "https://example.org/private" :callback callback}
                                opts)
                  (fn [report]
                    (is (= "The topic is private." (:denied report)))
                    (is (= {"id" "7" "hub.mode" "denied" "hub.topic" "https://example.org/private"
                            "hub.reason" "The topic is private."}
                           (query (peek @sent))))
                    (is (= {} @subscriptions))))
     #(hub/run-job! {:job :verify :mode :subscribe :topic topic :callback callback} opts)
     #(async/then (hub/run-job! {:job :deny :topic topic :callback callback :reason "Closed."} opts)
                  (fn [report]
                    (is (= 200 (:status report)))
                    (is (= "denied" (get (query (peek @sent)) "hub.mode")))
                    (is (= {} @subscriptions) "a denial at any time ends the subscription"))))))

(def rss
  (str "<rss version=\"2.0\"><channel><title>A show</title>"
       "<item><guid>2</guid><title>Two</title></item>"
       "<item><guid>1</guid><title>One</title></item>"
       "</channel></rss>"))

(defn- subscribed
  "The subscriptions of the topic, by callback, with a secret for `b`,
  until an hour after t0."
  []
  {topic {"https://a.example/cb" {:topic topic :callback "https://a.example/cb" :lease-seconds 3600
                                  :expires #inst "2026-10-10T13:00:00Z"}
          "https://b.example/cb" {:topic topic :callback "https://b.example/cb" :lease-seconds 3600
                                  :secret (secret/hide "s3cr3t") :expires #inst "2026-10-10T13:00:00Z"}}})

(deftest deliveries
  (let [subscriptions (atom (subscribed))
        sent          (atom [])
        jobs          (atom [])
        content       {:body (bytes/encode rss) :content-type "application/rss+xml"}
        opts          (options subscriptions (subscriber sent) jobs)
        to            (fn [cb] (some #(when (= cb (:url %)) %) @sent))]
    (run
     #(async/then (hub/run-job! {:job :distribute :topic topic :content content} opts)
                  (fn [report]
                    (is (= #{{:topic topic :callback "https://a.example/cb" :attempt 0 :status 202 :outcome :delivered}
                             {:topic topic :callback "https://b.example/cb" :attempt 0 :status 202 :outcome :delivered}}
                           (set (:deliveries report))))
                    (let [a (to "https://a.example/cb")
                          b (to "https://b.example/cb")]
                      (is (= [:post 0 rss] [(:method a) (:max-redirects a) (bytes/decode (:body a))])
                          "the whole content, to the exact callback")
                      (is (= {"Content-Type" "application/rss+xml"
                              "Link"         (str "<" hub-url ">; rel=\"hub\", <" topic ">; rel=\"self\"")}
                             (:headers a))
                          "the content type of the topic, and one Link header")
                      (is (websub/signed? "s3cr3t" (get-in b [:headers "X-Hub-Signature"]) (:body b) #{"sha256"})
                          "a signature of sha256 for the subscription with a secret"))
                    (is (= t0 (get-in @subscriptions [topic "https://a.example/cb" :published])))))
     #(async/then (hub/run-job! {:job :deliver :topic topic :callback "https://a.example/cb"
                                 :content (assoc content :published t0) :attempt 1}
                                opts)
                  (fn [report]
                    (is (= :stale (:outcome report)) "a retry of what the subscriber has had")))
     #(do (reset! sent [])
          (async/then (hub/run-job! {:job :distribute :topic topic :content content}
                                    (assoc opts
                                           :now-fn (constantly #inst "2026-10-10T12:30:00Z")
                                           :send (subscriber sent (fn [r]
                                                                    (if (str/starts-with? (:url r) "https://a.") 500 410)))))
                      (fn [report]
                        (is (= #{[:failed 500 #inst "2026-10-10T12:31:00Z"] [:gone 410 nil]}
                               (set (map (juxt :outcome :status :retry-at) (:deliveries report)))))
                        (is (= [{:job :deliver :topic topic :callback "https://a.example/cb" :attempt 1
                                 :at #inst "2026-10-10T12:31:00Z"}]
                               (map (fn [job] (dissoc job :content)) @jobs))
                            "a retry of the failure, for later")
                        (is (= ["https://a.example/cb"] (keys (get @subscriptions topic)))
                            "the subscription that failed stays, and the one that's gone doesn't"))))
     #(let [retry (assoc (peek @jobs) :attempt 5)]
        (reset! jobs [])
        (async/then (hub/run-job! retry (assoc opts :send (subscriber sent (constantly 503))))
                    (fn [report]
                      (is (= [:failed 503] ((juxt :outcome :status) report)))
                      (is (nil? (:retry-at report)))
                      (is (= [] @jobs) "no retry after the last"))))
     #(async/then (hub/run-job! {:job :deliver :topic topic :callback "https://b.example/cb" :content content :attempt 1}
                                opts)
                  (fn [report]
                    (is (= :unsubscribed (:outcome report)))))
     #(async/then (hub/run-job! {:job :distribute :topic topic :content content}
                                (assoc opts :now-fn (constantly #inst "2026-10-10T14:00:00Z")))
                  (fn [report]
                    (is (= [:expired] (map :outcome (:deliveries report))))
                    (is (= {} @subscriptions) "no delivery after the lease, which ends the subscription"))))))

(deftest jobs-as-edn
  (let [verify  {:job :verify :mode :subscribe :topic topic :callback callback :lease-seconds 3600
                 :secret (secret/hide "s3cr3t")}
        content {:body (bytes/encode rss) :content-type "application/rss+xml" :published t0}
        deliver {:job :deliver :topic topic :callback "https://a.example/cb" :attempt 1
                 :at #inst "2026-10-10T12:31:00Z" :content content}
        again   (comp hub/parse-job hub/format-job)]
    (testing "a secret, revealed in the EDN and hidden again"
      (is (str/includes? (hub/format-job verify) "s3cr3t"))
      (is (secret/hidden? (:secret (again verify))))
      (is (= (update verify :secret secret/reveal) (update (again verify) :secret secret/reveal))))
    (testing "the bytes of a topic, in base64url"
      (is (= rss (bytes/decode (get-in (again deliver) [:content :body]))))
      (is (= (update deliver :content dissoc :body) (update (again deliver) :content dissoc :body))))
    (is (= {:job :distribute :topic topic} (again {:job :distribute :topic topic})))
    (let [sent (atom [])
          opts (options (atom (subscribed)) (subscriber sent) (atom []))]
      (run
       #(async/then (hub/run-job! (again (assoc deliver :attempt 0)) opts)
                    (fn [report]
                      (is (= :delivered (:outcome report)) "and a job read back runs")
                      (is (= rss (bytes/decode (:body (peek @sent)))))))))))

#?(:clj
   (deftest deliveries-at-once
     (let [in-flight (atom 0)
           most      (atom 0)
           sent      (atom [])
           answer    (subscriber sent)
           slow      (fn [request]
                       (swap! most max (swap! in-flight inc))
                       (Thread/sleep 100)
                       (swap! in-flight dec)
                       (answer request))
           content   {:body (bytes/encode rss) :content-type "application/rss+xml"}
           most-at   (fn [more]
                       (reset! most 0)
                       (hub/run-job! {:job :distribute :topic topic :content content}
                                     (merge (options (atom (subscribed)) slow (atom [])) more))
                       @most)]
       (is (= 2 (most-at {})) "every subscriber at once, up to :concurrency")
       (is (= 1 (most-at {:concurrency 1})) "and no more"))))

(deftest deliveries-by-rules-of-the-hub
  (let [subscriptions (atom (subscribed))
        sent          (atom [])
        content       {:body (bytes/encode rss) :content-type "application/rss+xml" :published t0}
        opts          (assoc (options subscriptions (subscriber sent (constantly 410)) (atom []))
                             :delete-gone? false)]
    (run
     #(async/then (hub/run-job! {:job :distribute :topic topic :content content} opts)
                  (fn [report]
                    (is (= #{:gone} (set (map :outcome (:deliveries report)))))
                    (is (= 2 (count (get @subscriptions topic)))
                        "a subscription that is gone stays by :delete-gone?")))
     #(do (swap! subscriptions assoc-in [topic "https://a.example/cb" :published] t0)
          (async/then (hub/run-job! {:job :deliver :topic topic :callback "https://a.example/cb" :content content
                                     :attempt 1}
                                    (assoc opts :skip-stale? false :send (subscriber sent)))
                      (fn [report]
                        (is (= :delivered (:outcome report))
                            "a retry of what the subscriber has had, by :skip-stale?"))))
     #(async/then (hub/run-job! {:job :distribute :topic topic :content content}
                                (assoc opts :delete-expired? false :now-fn (constantly #inst "2026-10-10T14:00:00Z")))
                  (fn [report]
                    (is (= #{:expired} (set (map :outcome (:deliveries report)))))
                    (is (= 2 (count (get @subscriptions topic)))
                        "and one whose lease ended, by :delete-expired?"))))))

(defn- content-type
  "The Content-Type of the `request`."
  [request]
  (get-in request [:headers "Content-Type"]))

(deftest topics-that-the-hub-fetches
  (let [subscriptions (atom (subscribed))
        sent          (atom [])
        jobs          (atom [])
        topics        {topic                       {:status  200
                                                    :headers {"content-type" ["text/plain; charset=utf-8"]}
                                                    :body    (bytes/encode "Plain text.")}
                       "https://example.org/json" {:status  200
                                                    :headers {"content-type" ["application/json"]}
                                                    :body    (bytes/encode "{\"items\":[]}")}}
        send          (fn [request]
                        (if (= :post (:method request))
                          ((subscriber sent) request)
                          (merge {:url (:url request)} (get topics (:url request) {:status 404 :headers {}}))))
        opts          (options subscriptions send jobs)]
    (swap! subscriptions assoc "https://example.org/json" (update-vals (get (subscribed) topic) #(assoc % :topic "https://example.org/json")))
    (run
     #(async/then (hub/run-job! {:job :distribute :topic topic} opts)
                  (fn [_]
                    (is (= #{["text/plain; charset=utf-8" "Plain text."]}
                           (set (map (juxt content-type (comp bytes/decode :body)) @sent)))
                        "105: a topic of plain text")))
     #(do (reset! sent [])
          (async/then (hub/run-job! {:job :distribute :topic "https://example.org/json"} opts)
                      (fn [_]
                        (is (= #{"application/json"} (set (map content-type @sent)))
                            "106: a topic of JSON"))))
     #(async/then (hub/run-job! {:job :distribute :topic "https://example.org/gone"} opts)
                  (fn [report]
                    (is (= [404 #inst "2026-10-10T12:01:00Z"] [(get-in report [:error :status]) (:retry-at report)]))
                    (is (= [{:job :distribute :topic "https://example.org/gone" :attempt 1 :at #inst "2026-10-10T12:01:00Z"}]
                           @jobs)
                        "a topic that doesn't answer, tried again later"))))))

(defn- feed-of
  "The bytes of an RSS feed with an item of each of the `guids`."
  [& guids]
  (bytes/encode (str "<rss version=\"2.0\"><channel><title>A show</title>"
                     (apply str (for [g guids] (str "<item><guid>" g "</guid></item>")))
                     "</channel></rss>")))

(deftest diffs
  (let [subscriptions (atom (subscribed))
        sent          (atom [])
        opts          (assoc (options subscriptions (subscriber sent) (atom [])) :diff? true)
        guids         (fn [request]
                        (re-seq #"(?<=<guid>)[^<]+" (bytes/decode (:body request))))
        at            (fn [t] (constantly t))]
    (run
     #(async/then (hub/run-job! {:job :distribute :topic topic :content {:body (feed-of "1") :content-type "application/rss+xml"}}
                                opts)
                  (fn [_]
                    (is (= [["1"] ["1"]] (map guids @sent)) "everything the first time")))
     #(do (reset! sent [])
          (async/then (hub/run-job! {:job     :distribute :topic topic
                                     :content {:body (feed-of "2" "1") :content-type "application/rss+xml"}}
                                    (assoc opts :now-fn (at #inst "2026-10-10T12:10:00Z")))
                      (fn [_]
                        (is (= [["2"] ["2"]] (map guids @sent)) "then only the entries that are new")
                        (let [b (first (filter (fn [r] (str/starts-with? (:url r) "https://b.")) @sent))]
                          (is (websub/signed? "s3cr3t" (get-in b [:headers "X-Hub-Signature"]) (:body b))
                              "signed as it's sent")))))
     #(do (reset! sent [])
          (async/then (hub/run-job! {:job     :distribute :topic topic
                                     :content {:body (bytes/encode "{}") :content-type "application/json"}}
                                    (assoc opts :now-fn (at #inst "2026-10-10T12:20:00Z")))
                      (fn [_]
                        (is (= ["{}" "{}"] (map (comp bytes/decode :body) @sent)) "and all of a topic that is no feed")))))))

(deftest leases-and-retries
  (is (= 864000 (hub/lease-seconds nil)) "ten days by default")
  (is (= 3600 (hub/lease-seconds 60)))
  (is (= 86400 (hub/lease-seconds 86400)))
  (is (= 864000 (hub/lease-seconds 99999999)) "never longer than the hub's limit")
  (is (= 60 (hub/lease-seconds 60 (assoc hub/default-options :min-lease-seconds 10))) "or the limits of a hub")
  (is (= [#inst "2026-10-10T12:01:00Z" #inst "2026-10-10T12:05:00Z" #inst "2026-10-10T12:30:00Z"
          #inst "2026-10-10T14:00:00Z" #inst "2026-10-11T00:00:00Z" nil]
         (map #(hub/next-attempt % t0) (range 6))))
  (is (= [#inst "2026-10-10T12:00:10Z" nil] (map #(hub/next-attempt % t0 [10]) (range 2))))
  (is (hub/active? {:expires #inst "2026-10-10T12:00:01Z"} t0))
  (is (not (hub/active? {:expires t0} t0)))
  (let [a     (atom {})
        store (hub/memory-store a)]
    (is (= {:x 1} ((:update! store) topic callback (constantly {:x 1}))))
    (is (= [{:x 1}] ((:subscriptions store) topic)))
    (is (nil? ((:update! store) topic callback (constantly nil))))
    (is (= {} @a) "a topic without subscriptions is gone from the map")))

(deftest request-maps-by-the-spec
  (let [job {:job :verify :mode :unsubscribe :topic topic :callback callback}]
    (is (= {"id" "7" "hub.mode" "unsubscribe" "hub.topic" topic "hub.challenge" "abc"}
           (query (hub/verification job "abc")))
        "a check without a lease")
    (is (= {"id" "7" "hub.mode" "denied" "hub.topic" topic} (query (hub/denial job)))
        "a denial without a reason"))
  (let [subscription {:topic topic :callback callback :secret "s3cr3t"}
        r            (hub/distribution subscription {:body (bytes/encode rss)} hub-url)]
    (is (str/starts-with? (get-in r [:headers "X-Hub-Signature"]) "sha256=")
        "a delivery signed by sha256")))
