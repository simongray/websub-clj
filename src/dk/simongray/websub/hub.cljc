(ns dk.simongray.websub.hub
  "A WebSub hub: it takes subscriptions, checks each with its subscriber,
  and delivers each new version of a topic to the topic's subscribers.

  Nothing runs in the background. The Ring handler answers subscribers and
  publishers at once, and gives the work that follows to the :schedule!
  function of the options as a job, a map that run-job! does. A delivery
  that fails gives its retry to :schedule! in the same way, with the
  instant to run it as its :at. The subscriptions are in a store of yours,
  a map of two functions, or in memory-store.

  The code follows sections 5 to 8 of the W3C Recommendation of 2 June
  2026, https://www.w3.org/TR/websub/."
  (:require #?(:clj [clojure.edn :as edn] :cljs [cljs.reader :as edn])
            [clojure.string :as str]
            [dk.simongray.wary-fetch :as http]
            [dk.simongray.wary-fetch.async :as async]
            [dk.simongray.wary-fetch.bytes :as bytes]
            [dk.simongray.wary-fetch.headers :as headers]
            [dk.simongray.wary-fetch.secret :as secret]
            [dk.simongray.wary-fetch.url :as url]
            [dk.simongray.websub :as websub]
            [dk.simongray.websub.document :as document]
            [dk.simongray.websub.ring :as ring]
            [dk.simongray.whatwg-mime :as mime]))

;; WebSub 5.1.1: decode the characters that need no percent-encoding, which
;; RFC 3986, section 2.3, names
(defn ^:no-doc normalized
  "The `url` with each unreserved character that is percent-encoded
  decoded."
  [url]
  (str/replace url
               #"%([0-9A-Fa-f]{2})"
               (fn [[escape hex]]
                 (let [code #?(:clj  (Long/parseLong hex 16)
                               :cljs (js/parseInt hex 16))
                       c    (str (char code))]
                   (if (re-matches #"[A-Za-z0-9._~-]" c) c escape)))))

;; WebSub 5.3.1
(defn verified?
  "Whether the subscriber's `response` confirms the check with the
  `challenge`: a success whose body is the challenge."
  [challenge {:keys [status body] :as response}]
  (boolean (and status
                (<= 200 status 299)
                (= challenge body))))

(def default-options
  "The default of each option of a hub that has one, which follows WebSub:

  - :lease-seconds, the lease of a subscription that asks for none, ten
    days, as WebSub 8.2 suggests
  - :min-lease-seconds and :max-lease-seconds, the bounds of a lease, an
    hour and ten days
  - :retries, the seconds to wait before each new attempt at a delivery
    that failed: a minute, then 5, 30, 120 and 720
  - :signature-method, sha256, as WebSub 8.3 advises
  - :diff?, whether a subscriber gets only the entries of an Atom or RSS
    feed that it hasn't had, false
  - :accept-pred, a function of a job and the request that gave it, which
    says whether the hub takes it, always true
  - :secret-pred, a predicate of the secret of a subscription, which takes
    a text of fewer than 200 bytes, as WebSub 5.1 says
  - :url-fn, a function of the callback and topic URLs of a subscription,
    and the topic URLs of a publisher, normalized
  - :topic-fields, the fields that name the topics of a publisher, in the
    order that they count: hub.url and hub.topic
  - :challenge-fn, a function that gives the challenge of each check,
    websub/token
  - :verified-pred, a function of the challenge and the subscriber's
    response, which says whether the check passed, verified?
  - :callback-redirects, how many redirects the requests to a callback
    follow, none, as WebSub 5.3.1 and 8.3 say
  - :delete-gone?, whether a 410 from a subscriber deletes its
    subscription, which WebSub 7 allows, true
  - :delete-expired?, whether a subscription is deleted once its lease
    ends, true
  - :skip-stale?, whether a retry is left out once the subscriber has had
    newer content, true
  - :max-form-bytes, the limit of a request to the hub, 64 kB
  - :max-bytes, the limit of a topic's content, 32 MB
  - :concurrency, how many deliveries of a topic are under way at once,
    16, so that a topic with thousands of subscribers doesn't open
    thousands of connections
  - :allow-pred, url/public?, so that no request goes to the hub's own
    network
  - :send, http/send!
  - :now-fn, a function that gives the instant now"
  {:lease-seconds      864000
   :min-lease-seconds  3600
   :max-lease-seconds  864000
   :retries            [60 300 1800 7200 43200]
   :signature-method   "sha256"
   :diff?              false
   :accept-pred        (constantly true)
   :secret-pred        websub/secret?
   :url-fn             normalized
   :topic-fields       ["hub.url" "hub.topic"]
   :challenge-fn       websub/token
   :verified-pred      verified?
   :callback-redirects 0
   :delete-gone?       true
   :delete-expired?    true
   :skip-stale?        true
   :max-form-bytes     65536
   :max-bytes          (* 32 1024 1024)
   :concurrency        16
   :allow-pred         url/public?
   :send               http/send!
   :now-fn             #?(:clj  #(java.util.Date.)
                          :cljs #(js/Date.))})

(defn ^:no-doc options
  "The `opts` of a hub over default-options. They must name the hub's
  :url, its :store and :schedule!."
  [{:keys [url store schedule!] :as opts}]
  (when-not (and (url/web-url? url)
                 (fn? (:subscriptions store))
                 (fn? (:update! store))
                 (fn? schedule!))
    (throw (ex-info "A hub needs its :url, a :store and :schedule!" {})))
  (merge default-options opts))

(defn active?
  "Whether the `subscription` is active at the instant `at`, before its
  lease ends."
  [subscription at]
  ;; WebSub 5.3.1: a hub enforces the end of a lease
  (< (inst-ms at) (inst-ms (:expires subscription))))

;; WebSub 5.3.1 and 8.2
(defn lease-seconds
  "The lease of a subscription whose request asked for `asked` seconds, or
  for none when nil, by the lease options of `opts`, those of
  default-options by default. It's never endless."
  ([asked]
   (lease-seconds asked default-options))
  ([asked {:keys [lease-seconds min-lease-seconds max-lease-seconds]
           :as   opts}]
   (-> (or asked lease-seconds)
       (max min-lease-seconds)
       (min max-lease-seconds))))

(defn next-attempt
  "When to try again, after the failure of the attempt numbered `attempt`,
  from 0, at the instant `at`, by the seconds to wait in `retries`, those
  of default-options by default. Nil when the retries are used up."
  ([attempt at]
   (next-attempt attempt at (:retries default-options)))
  ([attempt at retries]
   ;; WebSub 7: a hub retries within limits of its own
   (some->> (get retries attempt) (websub/later at))))

(defn- without
  "The map `m` of subscriptions by topic and callback without the one of
  `topic` and `callback`."
  [m topic callback]
  (let [m (update m topic dissoc callback)]
    (if (empty? (get m topic))
      (dissoc m topic)
      m)))

(defn memory-store
  "A store of subscriptions in the atom `a`, or in a new one, as a map by
  topic and then by callback.

  A store is a map of two functions, which can give their results as
  promises in ClojureScript:

  - :subscriptions, a function of a topic that gives its subscriptions
  - :update!, a function of a topic, a callback and a function f, which
    replaces their subscription with what f gives for it, nil for none,
    in one step"
  ([]
   (memory-store (atom {})))
  ([a]
   {:subscriptions (fn [topic]
                     (vals (get @a topic)))
    :update!       (fn [topic callback f]
                     (-> (swap! a (fn [m]
                                    (if-let [s (f (get-in m [topic callback]))]
                                      (assoc-in m [topic callback] s)
                                      (without m topic callback))))
                         (get-in [topic callback])))}))

(defn- subscription-of
  "The subscription of `topic` and `callback` in the `store`."
  [{:keys [subscriptions] :as store} topic callback]
  (async/then (subscriptions topic)
              (fn [xs]
                (some #(when (= callback (:callback %)) %) xs))))

(defn- exchange!
  "The response to the `request`, sent with the :send of `opts` and their
  limits, or a map of the :error as http/failure has it."
  [request {:keys [send] :as opts}]
  (async/then (http/try-send! send (merge (http/with-options {} opts)
                                          request))
              (fn [{:keys [value error]}]
                (or value
                    {:error (http/failure error)}))))

(defn- send-to-callback!
  "Send the `request` to a callback by `opts`, which follows as many
  redirects as their :callback-redirects."
  [request {:keys [callback-redirects] :as opts}]
  (exchange! (assoc request :max-redirects callback-redirects) opts))

(defn- values
  "The values of the field `k` in the form `fields`."
  [fields k]
  (keep (fn [[n v]] (when (= k n) v)) fields))

(defn- field
  "The value of the field `k` in the form `fields`, the last when there are
  several, or nil for an empty one."
  [fields k]
  (not-empty (last (values fields k))))

;; WebSub 5.1 and 5.1.1
(defn- subscription-jobs
  "The job of the subscription request with the form `fields` and `mode`,
  as a map of the :jobs, or the :problem, by `opts`."
  [fields mode {:keys [url-fn secret-pred] :as opts}]
  (let [callback  (some-> (field fields "hub.callback") url-fn)
        topic     (some-> (field fields "hub.topic") url-fn)
        lease     (field fields "hub.lease_seconds")
        seconds   (some-> lease parse-long)
        secret    (field fields "hub.secret")
        subscribe (= "subscribe" mode)
        job       (cond-> {:job      :verify
                           :mode     (keyword mode)
                           :topic    topic
                           :callback callback}
                    (and subscribe seconds)
                    (assoc :lease-seconds seconds)

                    (and subscribe secret)
                    (assoc :secret (secret/hide secret)))]
    (cond
      (not (url/web-url? callback))
      {:problem "hub.callback must be an http or https URL"}

      (not (url/web-url? topic))
      {:problem "hub.topic must be an http or https URL"}

      ;; WebSub 5.1: the lease of an unsubscription is ignored
      (and subscribe lease (not (pos-int? seconds)))
      {:problem "hub.lease_seconds must be a positive integer"}

      (and secret (not (secret-pred secret)))
      {:problem "hub.secret is not one that this hub takes"}

      :else
      {:jobs [job]})))

;; WebSub 6: the form that most hubs take, with hub.url or hub.topic
(defn- distribution-jobs
  "The jobs of distributing the topics that the publisher's form `fields`
  names, as a map of the :jobs, or the :problem, by `opts`."
  [fields {:keys [url-fn topic-fields] :as opts}]
  (let [topics (map url-fn (some #(seq (values fields %)) topic-fields))]
    (cond
      (empty? topics)
      {:problem (str (first topic-fields) " must name the topic that changed")}

      (not (every? url/web-url? topics))
      {:problem (str (first topic-fields) " must be an http or https URL")}

      :else
      {:jobs (mapv #(hash-map :job :distribute :topic %) (distinct topics))})))

(defn- form-jobs
  "The jobs of the request to the hub with the form `fields`, as a map of
  the :jobs, or the :problem, by `opts`."
  [fields opts]
  (let [mode (field fields "hub.mode")]
    (case mode
      ("subscribe" "unsubscribe") (subscription-jobs fields mode opts)
      "publish"                   (distribution-jobs fields opts)
      {:problem "hub.mode must be subscribe, unsubscribe or publish"})))

;; WebSub 5.1.2: the reason for an error, in plain text
(defn- text
  "The Ring response of `status` with the plain text `s`."
  [status s]
  (ring/response status "text/plain; charset=utf-8" s))

(defn- answer
  "The hub's answer to the POST `request`, by `opts`, once it has given the
  jobs of the request to :schedule!."
  [{:keys [schedule! accept-pred max-form-bytes] :as opts} request]
  (let [fields                 (ring/form-fields request max-form-bytes)
        {:keys [jobs problem]} (some-> fields (form-jobs opts))]
    (cond
      (nil? fields)
      (text 413 "The request is too large")

      problem
      (text 400 problem)

      (not-every? #(accept-pred % request) jobs)
      (text 403 "The hub doesn't take this request")

      ;; WebSub 5.1.2: the answer doesn't wait for the check
      :else
      (do (doseq [job jobs]
            (schedule! job))
          (text 202 "")))))

(defn handler
  "A Ring handler for the URL of the hub, by `opts`, for the requests of
  subscribers and publishers.

  It answers 202 to a request that it takes, and gives the job that
  follows to :schedule!: a check of a subscription or an unsubscription,
  or the delivery of a topic that a publisher says has changed. It
  answers 400 with the reason in plain text to a request that's wrong, and
  403 to one that the :accept-pred refuses.

  A publisher names the topic in the :topic-fields, hub.url as most hubs
  take it, or hub.topic. The `opts` are those of run-job!, and :moved-to,
  the URL of another hub that takes the requests from now on."
  [opts]
  (let [{:keys [moved-to] :as opts} (options opts)]
    (ring/handler
     (fn [request]
       (cond
         ;; WebSub 5.1.2: a hub can send a request on to another hub
         moved-to
         (assoc-in (text 308 "") [:headers "Location"] moved-to)

         (= :post (:request-method request))
         (answer opts request)

         ;; RFC 9110, section 15.5.6
         :else
         (assoc-in (text 405 "") [:headers "Allow"] "POST"))))))

;; WebSub 5.3 and 5.1.1
(defn verification
  "The request map that asks the subscriber of the `job` whether it wants
  it, with the `challenge` to give back and, for a subscription, its lease
  of `lease-seconds`."
  ([job challenge]
   (verification job challenge nil))
  ([{:keys [mode topic callback] :as job} challenge lease-seconds]
   (let [params [["hub.mode" (name mode)]
                 ["hub.topic" topic]
                 ["hub.challenge" challenge]
                 ["hub.lease_seconds" lease-seconds]]]
     {:method        :get
      :url           (url/with-query callback params)
      ;; WebSub 5.3.1 and 8.3: the exact callback, and a redirect fails
      :max-redirects 0})))

;; WebSub 5.2
(defn denial
  "The request map that tells the subscriber of the `job` that the hub
  denies its subscription, for the `reason`, which can be left out."
  ([job]
   (denial job nil))
  ([{:keys [topic callback] :as job} reason]
   {:method        :get
    :url           (url/with-query callback [["hub.mode" "denied"]
                                             ["hub.topic" topic]
                                             ["hub.reason" reason]])
    :max-redirects 0}))

(defn- reported
  "The `report` with the :status of the subscriber's `response`, or its
  :error."
  [report response]
  (merge report (select-keys response [:status :error])))

(defn- tell-denied!
  "Tell the subscriber of the `job` that the hub denies its subscription,
  for the `reason`, by `opts`, and give the job's report."
  [job reason opts]
  (async/then (send-to-callback! (denial job reason) opts)
              #(reported (assoc job :denied reason) %)))

(defn- subscribed
  "A function of a subscription that replaces it with the one that the
  `job` asked for, with a lease of `lease-seconds` from the instant `at`.
  What the old one was delivered stays."
  [{:keys [topic callback secret] :as job} lease-seconds at]
  (fn [old]
    (merge (select-keys old [:published :delivered])
           {:topic         topic
            :callback      callback
            :secret        secret
            :lease-seconds lease-seconds
            :expires       (websub/later at lease-seconds)})))

(defn- check!
  "Check with the subscriber that it asked for the `job`, by `opts`, and
  save or delete the subscription when it did."
  [{:keys [mode topic callback] :as job}
   {:keys [store now-fn challenge-fn verified-pred] :as opts}]
  (let [at        (now-fn)
        challenge (challenge-fn)
        lease     (when (= :subscribe mode)
                    (lease-seconds (:lease-seconds job) opts))
        change    (if lease
                    (subscribed job lease at)
                    (constantly nil))]
    (async/then (send-to-callback! (verification job challenge lease) opts)
                (fn [response]
                  (let [ok?    (boolean (verified-pred challenge response))
                        report (cond-> (reported job response)
                                 true  (assoc :verified? ok?)
                                 lease (assoc :lease-seconds lease))]
                    ;; WebSub 5.1: a check that fails changes nothing
                    (if ok?
                      (async/then ((:update! store) topic callback change)
                                  (constantly report))
                      report))))))

(defn verify!
  "Check with the subscriber that it asked for the subscription or the
  unsubscription of the `job`, by `opts`, and save or delete the
  subscription when it did.

  A subscription goes to the :denial-fn of `opts` first, if there is one,
  a function of the job that gives a reason to deny it, or nil. A denied
  subscription is told so and isn't saved. It gives the job with
  :verified? and the subscriber's :status or the :error, or with the
  :denied reason."
  [{:keys [mode] :as job} opts]
  (let [{:keys [denial-fn] :as opts} (options opts)]
    ;; WebSub 5.2: only a subscription is validated
    (async/then (when (and denial-fn (= :subscribe mode))
                  (denial-fn job))
                (fn [reason]
                  (if reason
                    (tell-denied! job reason opts)
                    (check! job opts))))))

;; WebSub 5.2: a hub may deny a subscription at any time
(defn deny!
  "Delete the subscription of the topic and callback of the `job`, by
  `opts`, and tell the subscriber that the hub denies it for the :reason
  of the job. It gives the job with the subscriber's :status, or the
  :error."
  [{:keys [topic callback reason] :as job} opts]
  (let [{:keys [store] :as opts} (options opts)]
    (async/then ((:update! store) topic callback (constantly nil))
                (fn [_]
                  (tell-denied! job reason opts)))))

;; WebSub 7, 7.1 and 8.3
(defn distribution
  "The request map that delivers the `content` of a topic, a map of its
  :body and :content-type, to the `subscription` from the hub at
  `hub-url`, signed by `method`, sha256 by default, when the subscription
  has a secret."
  ([subscription content hub-url]
   (distribution subscription content hub-url
                 (:signature-method default-options)))
  ([{:keys [topic callback secret] :as subscription}
    {:keys [body content-type] :as content}
    hub-url
    method]
   {:method        :post
    :url           callback
    :headers       (cond-> {"Link" (websub/link-header [hub-url] topic)}
                     content-type (assoc "Content-Type" content-type)
                     secret       (assoc "X-Hub-Signature"
                                         (websub/signature method secret body)))
    :body          body
    :max-redirects 0}))

(defn- stale?
  "Whether the `subscription` has had the `content`, or a newer one."
  [{:keys [published] :as subscription} content]
  (boolean (and published
                (:published content)
                (<= (inst-ms (:published content)) (inst-ms published)))))

(defn- payload
  "What to send the `subscription` of the `content`: all of it, or by the
  :diff? of `opts` only the entries of a feed that it hasn't had, with
  the :ids of all."
  [{:keys [delivered] :as subscription}
   {:keys [body content-type] :as content}
   {:keys [diff?] :as opts}]
  (let [charset (mime/charset content-type)
        fewer   (when diff?
                  (document/diff body charset (or delivered #{})))]
    (cond-> content
      fewer (assoc :body (:body fewer) :ids (:ids fewer)))))

(defn- delivered
  "A function of a subscription that records that it had the `content`, by
  the `sent` payload."
  [content sent]
  (fn [subscription]
    (when subscription
      (cond-> (assoc subscription :published (:published content))
        (:ids sent) (assoc :delivered (:ids sent))))))

(defn- retry!
  "Give the retry of the delivery of the `report` and the `content` to the
  :schedule! of `opts`, if any retries are left, and give the report with
  its :retry-at."
  [{:keys [topic callback attempt] :as report} content
   {:keys [retries schedule! now-fn] :as opts}]
  (if-let [retry-at (next-attempt attempt (now-fn) retries)]
    (do (schedule! {:job      :deliver
                    :topic    topic
                    :callback callback
                    :content  content
                    :attempt  (inc attempt)
                    :at       retry-at})
        (assoc report :retry-at retry-at))
    report))

(defn- outcome
  "The `report` of the delivery of the `sent` payload of `content`, by the
  subscriber's `response`, once the store of `opts` has what it means."
  [{:keys [topic callback] :as report} content sent response
   {:keys [store delete-gone?] :as opts}]
  (let [update! (:update! store)
        status  (:status response)
        report  (reported report response)]
    (cond
      (and status (<= 200 status 299))
      (async/then (update! topic callback (delivered content sent))
                  (constantly (assoc report :outcome :delivered)))

      ;; WebSub 7: a 410 says that the subscription was deleted
      (= 410 status)
      (async/then (when delete-gone?
                    (update! topic callback (constantly nil)))
                  (constantly (assoc report :outcome :gone)))

      ;; WebSub 7: anything else fails, and the subscription stays
      :else
      (retry! (assoc report :outcome :failed) content opts))))

(defn- deliver-to!
  "Deliver the `content` to the `subscription` by `opts`, as the attempt
  numbered `attempt`, and give the report."
  [{:keys [topic callback] :as subscription} content attempt
   {:keys [store url signature-method now-fn delete-expired? skip-stale?]
    :as   opts}]
  (let [at     (now-fn)
        report {:topic topic :callback callback :attempt attempt}]
    (cond
      ;; WebSub 5.3.1: no delivery after the lease
      (not (active? subscription at))
      (async/then (when delete-expired?
                    ((:update! store) topic callback
                                      #(when (and % (active? % at)) %)))
                  (constantly (assoc report :outcome :expired)))

      ;; a retry after a newer delivery would undo it
      (and skip-stale? (stale? subscription content))
      (assoc report :outcome :stale)

      :else
      (let [sent    (payload subscription content opts)
            request (distribution subscription sent url signature-method)]
        (async/then (send-to-callback! request opts)
                    #(outcome report content sent % opts))))))

(defn deliver!
  "Deliver the :content of the `job` to the subscriber of its :topic and
  :callback again, by `opts`, after a failed :attempt. It gives the
  report of the delivery, as distribute! has them."
  [{:keys [topic callback content attempt] :as job} opts]
  (let [{:keys [store] :as opts} (options opts)]
    (async/then (subscription-of store topic callback)
                (fn [subscription]
                  (if subscription
                    (deliver-to! subscription content attempt opts)
                    {:topic    topic
                     :callback callback
                     :attempt  attempt
                     :outcome  :unsubscribed})))))

(defn- fetch!
  "Fetch the content of the `topic` by `opts`, as a map of its :body and
  :content-type, or of the :error."
  [topic opts]
  (async/then (exchange! {:url topic :as :bytes} opts)
              (fn [{:keys [status headers body error] :as response}]
                (cond
                  error
                  {:error error}

                  (<= 200 status 299)
                  {:body         (or body (bytes/encode ""))
                   :content-type (headers/header headers "content-type")}

                  :else
                  {:error {:reason (str "The topic answered " status)
                           :status status}}))))

(defn- deliver-all!
  "Deliver the `content` to each subscriber of the `topic` by `opts`, at
  most :concurrency at once, and give their reports."
  [topic content {:keys [store concurrency] :as opts}]
  (async/then ((:subscriptions store) topic)
              (fn [subscriptions]
                (async/in-groups #(deliver-to! % content 0 opts)
                                 concurrency
                                 subscriptions))))

;; WebSub 7
(defn distribute!
  "Deliver the new content of the topic of the `job` to each of its
  subscribers, by `opts`.

  The content is the job's :content, a map of the :body bytes and the
  :content-type, or else what the topic gives when the hub fetches it.
  Each delivery gives a report of its :outcome: :delivered, :gone for a
  subscription that the subscriber deleted, :failed with the :retry-at of
  its retry, or :expired or :stale when there was nothing to deliver. It
  gives the job with the :deliveries, or with the :error of a fetch, and
  then the fetch's retry goes to :schedule!."
  [{:keys [topic content attempt] :or {attempt 0} :as job} opts]
  (let [{:keys [now-fn retries schedule!] :as opts} (options opts)
        at                                          (now-fn)
        report                                      (dissoc job :content)]
    (async/then
     (or content (fetch! topic opts))
     (fn [{:keys [error] :as content}]
       (if-not error
         (async/then (deliver-all! topic
                                   (update content :published #(or % at))
                                   opts)
                     #(assoc report :deliveries %))
         (let [retry-at (next-attempt attempt at retries)]
           (when retry-at
             (schedule! (assoc job :attempt (inc attempt) :at retry-at)))
           (cond-> (assoc report :error error)
             retry-at (assoc :retry-at retry-at))))))))

(defn run-job!
  "Do the `job` by `opts`, with verify!, distribute!, deliver! or deny! by
  its :job, and give the report. In ClojureScript, it gives a promise.

  The `opts` are those of default-options, and these:

  - :url, the URL of the hub
  - :store, the store of the subscriptions, e.g. memory-store
  - :schedule!, a function of a job that runs it with run-job! at its :at,
    or soon when it has none
  - :denial-fn, a function of a subscription's job that gives a reason to
    deny it, or nil"
  [job opts]
  (case (:job job)
    :verify     (verify! job opts)
    :distribute (distribute! job opts)
    :deliver    (deliver! job opts)
    :deny       (deny! job opts)))

;; A job holds a hidden secret and the bytes of a topic, which EDN can't
;; print, so they're text and base64url in the EDN of a job
(defn format-job
  "The `job` as EDN, e.g. for a queue in a database, with its :secret
  revealed and the bytes of its content in base64url. Keep it as safe as
  the subscriptions, since it can hold a subscriber's secret."
  [job]
  (binding [*print-length* nil
            *print-level*  nil]
    (pr-str (cond-> job
              (:secret job)
              (update :secret secret/reveal)

              (some? (get-in job [:content :body]))
              (update-in [:content :body] bytes/base64url)))))

(defn parse-job
  "The job in the EDN `s` that format-job gives."
  [s]
  (let [job (edn/read-string s)]
    (cond-> job
      (:secret job)
      (update :secret secret/hide)

      (string? (get-in job [:content :body]))
      (update-in [:content :body] bytes/unbase64url))))

#?(:clj
   (comment
     (def subscriptions
       (atom {}))

     (declare opts)

     (defn schedule!
       "Run the `job` on a thread of its own, at its :at."
       [job]
       (future
         (when-let [at (:at job)]
           (Thread/sleep (max 0 (- (inst-ms at) (System/currentTimeMillis)))))
         (prn (run-job! job opts))))

     (def opts
       {:url       "https://hub.example.org/"
        :store     (memory-store subscriptions)
        :schedule! schedule!})

     (def serve
       (handler opts))

     (serve {:request-method :post
             :form-params    {"hub.mode"     "subscribe"
                              "hub.topic"    "https://example.org/feed"
                              "hub.callback" "https://app.example.net/cb"}})

     ;; a hub inside a publisher delivers its own content
     (run-job! {:job     :distribute
                :topic   "https://example.org/feed"
                :content {:body         (bytes/encode "<rss/>")
                          :content-type "application/rss+xml"}}
               opts)
     #_.))
