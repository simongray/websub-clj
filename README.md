websub-clj
==========

[![Clojars Project](https://img.shields.io/clojars/v/dk.simongray/websub-clj.svg)](https://clojars.org/dk.simongray/websub-clj)

This is a Clojure and ClojureScript implementation of
[WebSub](https://www.w3.org/TR/websub/), the W3C standard for pushing a
feed or a page to its subscribers through a hub as soon as it changes, so
that they don't have to poll it. It covers all three roles of the
standard: the subscriber, the publisher and the hub.

- **The whole standard.** The code follows the Recommendation of 2 June
  2026 and cites the section of each rule it implements. Its tests
  include the scenarios of the [websub.rocks](https://websub.rocks) test
  suite, run locally.
- **Plain data.** Requests are maps that any HTTP client can send, and
  both a subscriber's callback and the hub are Ring handlers.
- **Nothing in the background.** The hub gives its work to you as jobs,
  plain maps, and you run them when and where you like.
- **Safe by default.** A secret only goes over https, signatures are
  compared in constant time, and the hub only sends to public addresses,
  never to its own network.

The same code runs on the JVM, in Node and in the browser. A page can
discover the hub of a topic and publish to it, with a `:send` of your own
where CORS stops the request, but it can't subscribe, since a subscriber
needs a callback URL that the hub can reach.

> This library was spun out of [podcast-clj](https://github.com/simongray/podcast-clj),
> a library for making podcast software, where servers use it to get new
> episodes pushed to them instead of polling every feed. Like podcast-clj,
> it was developed with assistance from frontier LLMs.

Getting started
---------------

It requires Clojure 1.11+ and Java 11+. Add it from
[Clojars](https://clojars.org/dk.simongray/websub-clj) to the `:deps` in
your `deps.edn`:

```clojure
dk.simongray/websub-clj {:mvn/version "0.7.1"}
```

For unreleased changes, depend on the latest commit on `master` instead.
In shadow-cljs, a Git dependency only works through `deps.edn`, so set
`:deps true` in your `shadow-cljs.edn` too. To read feeds in Node, set
`globalThis.DOMParser` to a DOMParser, e.g. the one from
`@xmldom/xmldom`.

Subscribing
-----------

### Finding the hub

Find the hubs of a topic with `websub/discover!`:

```clojure
(require '[dk.simongray.websub :as websub])

(websub/discover! "https://simon.grays.blog/feed")
;; => {:status 200
;;     :hubs   ["https://pubsubhubbub.superfeedr.com/"]
;;     :topic  "https://simon.grays.blog/feed"}
```

If you fetch the topic yourself, give the response to `websub/discover`
instead, for the same map.

WebSub takes the topic from a feed's `rel="self"` link, and many RSS
feeds leave it out. The map then has the hubs but no topic. To take the
URL that you fetched as the topic instead, give `{:url-as-topic? true}`.

### Subscribing at a hub

Subscribe to the topic at one of its hubs, with a callback URL that
nobody can guess and a secret for the hub to sign what it sends:

```clojure
(def secret
  (websub/token))

(websub/call! (websub/request :subscribe
                              "https://pubsubhubbub.superfeedr.com/"
                              "https://simon.grays.blog/feed"
                              (str "https://app.example.net/websub/" (websub/token))
                              {:secret secret}))
;; => {:status 202 :url "https://pubsubhubbub.superfeedr.com/"}
```

A secret only goes over https, so a hub with an http URL gets the request
at its https URL. When that fails, the call throws
`:dk.simongray.websub/https-failed`, and `{:secret-over-http? true}`
sends the request over http instead.

### Receiving deliveries

Serve the callback URL with a Ring handler. The hub first checks that you
asked for the subscription, and then it delivers each new version of the
topic:

```clojure
(def deliveries
  (atom []))

(def callback
  (websub/handler {:wanted-pred (fn [{:keys [topic]}]
                                  (= topic "https://simon.grays.blog/feed"))
                   :deliver!    (fn [{:keys [body]}]
                                  (swap! deliveries conj body))
                   :secret      secret}))
```

The `:wanted-pred` says whether you asked for a subscription to a topic.
The `:deliver!` function gets the bytes of each delivery that the hub
signed with your secret, so return from it quickly, and read the feed
afterwards.

A hub names the topic of a delivery in a Link header, and when it sends
none, the delivery has no topic. So give each subscription a callback URL
of its own, which the `:request` of a delivery has.

### Renewing

A hub keeps a subscription for the length of its lease, so you subscribe
again before the lease ends. To keep track of the leases, keep your
subscriptions in a map by an id of your own, e.g. the feed's. Record each
request with `websub/asked` before you send it, and take the handler's
options from the map:

```clojure
(def subscriptions
  (atom {}))

(defn now
  "The instant now."
  []
  (java.util.Date.))

(defn subscribe!
  "Ask the hub with the `request` for the subscription of `id`."
  [id request]
  (swap! subscriptions websub/asked id request (now))
  (websub/call! request))

(def callback
  (websub/handler
   (merge (websub/handler-options #(deref subscriptions))
          {:deliver!  (fn [{:keys [body request]}]
                        (let [[id] (websub/subscription-of @subscriptions
                                                           request)]
                          (swap! deliveries conj [id body])))
           :verified! #(swap! subscriptions websub/verified % (now))
           :denied!   #(swap! subscriptions websub/denied % (now))})))
```

The handler then confirms only the requests in the map, and it updates
the map with `websub/verified` and `websub/denied`. A confirmed
subscription is `:active`, with its lease and the time to renew it. A
denied one is `:denied`, since WebSub says that a denial is final.

Every hour or so, send the requests that `websub/due` gives:

```clojure
(defn new-callback
  "A new callback URL for the `subscription` of `id`."
  [id subscription]
  (str "https://app.example.net/websub/" (websub/token)))

(doseq [[id request] (websub/due @subscriptions (now) new-callback)]
  (subscribe! id request))
```

That renews each subscription whose time has come at a new callback URL,
as WebSub advises. It also asks again for subscriptions whose lease ran
out, or whose request the hub hasn't checked within an hour. The old
callback takes deliveries until the hub confirms the new one, and after
that it answers 410, which ends the old subscription. A topic can move,
though, so to follow it, discover the topic again and subscribe with a
request of your own.

To keep the map in a file or a database, write it as EDN with
`websub/format-subscriptions`, which reveals the secrets, and read it back
with `websub/parse-subscriptions`.

Publishing
----------

Name the hub in a Link header of the topic, or in the feed or page itself
with the links of `websub/links`, and tell the hub each time the topic
changes:

```clojure
(websub/link-header ["https://pubsubhubbub.superfeedr.com/"]
                    "https://simon.grays.blog/feed")
;; => "<https://pubsubhubbub.superfeedr.com/>; rel=\"hub\", <https://simon.grays.blog/feed>; rel=\"self\""

(websub/publish! ["https://pubsubhubbub.superfeedr.com/"]
                 "https://simon.grays.blog/feed")
;; => [{:hub "https://pubsubhubbub.superfeedr.com/" :status 204}]
```

Running a hub
-------------

A hub consists of its Ring handler, a store of subscriptions and a
function of yours that runs its jobs, e.g. on a thread pool or from a
queue in a database:

```clojure
(require '[dk.simongray.websub.hub :as hub])

(declare opts)

(defn schedule!
  "Run the `job` at its :at, on a thread of its own."
  [job]
  (future
    (when-let [at (:at job)]
      (Thread/sleep (max 0 (- (inst-ms at) (System/currentTimeMillis)))))
    (hub/run-job! job opts)))

(def opts
  {:url       "https://hub.example.net/"
   :store     (hub/memory-store)
   :schedule! schedule!})

(def hub-handler
  (hub/handler opts))
```

The handler answers subscribers and publishers right away. The work that
follows, the check of a subscription or the delivery of a topic that
changed, goes to your `schedule!` as a job. A delivery that fails comes
back as a retry, with the `:at` to run it.

A store is a map of two functions, so one over a database takes a few
lines. The defaults of the options, e.g. the leases and the retries, are
in `hub/default-options`, and the docstrings of `hub/handler` and
`hub/run-job!` describe the rest.

### Jobs in a database

A queue in a database keeps the jobs through a restart. Write a job as
text with `hub/format-job`, which reveals its secret and puts its bytes
in base64url, and read it back with `hub/parse-job`. For example, with
[next.jdbc](https://github.com/seancorfield/next-jdbc) and this table in
PostgreSQL:

```sql
CREATE TABLE hub_jobs (id     BIGSERIAL PRIMARY KEY,
                       run_at TIMESTAMPTZ NOT NULL,
                       job    TEXT NOT NULL);
```

```clojure
(require '[next.jdbc :as jdbc])

(defn schedule!
  "Queue the `job` in the database `db`, to run at its :at, or at once."
  [db job]
  (jdbc/execute! db ["INSERT INTO hub_jobs (run_at, job) VALUES (?, ?)"
                     (or (:at job) (java.util.Date.))
                     (hub/format-job job)]))

(defn run-due-jobs!
  "Run the jobs in the database `db` that are due, by the hub's `opts`."
  [db opts]
  (doseq [{:hub_jobs/keys [id job]}
          (jdbc/execute! db [(str "SELECT id, job FROM hub_jobs"
                                  " WHERE run_at <= ? ORDER BY run_at")
                             (java.util.Date.)])]
    (hub/run-job! (hub/parse-job job) opts)
    (jdbc/execute! db ["DELETE FROM hub_jobs WHERE id = ?" id])))
```

Give the hub `(partial schedule! db)` as its `:schedule!`, and call
`run-due-jobs!` from one worker every few seconds. A job is deleted once
it has run, so one that a crash stops runs again. The text holds the
secrets of subscriptions, so keep the table as safe as the store.

Principles
----------

- **Standards first.** The code follows the Recommendation and cites its
  sections, and a comment marks each choice of its own. Most rules are
  options too, and `websub/default-options` and `hub/default-options`
  list their defaults, which follow the spec.
- **Plain data.** Requests, jobs and reports are maps, and every
  function that sends takes a `:send` of your own.
- **One codebase.** The library is written in `.cljc` and depends on
  [wary-fetch](https://github.com/simongray/wary-fetch) for requests,
  [webcrypto-clj](https://github.com/simongray/webcrypto-clj) for their
  signatures,
  [qname-hiccup](https://github.com/simongray/qname-hiccup) for feeds,
  [html-pieces](https://github.com/simongray/html-pieces) for pages,
  [whatwg-url](https://github.com/simongray/whatwg-url) for their URLs and
  [whatwg-mime](https://github.com/simongray/whatwg-mime) to tell a feed
  from a page.

Development
-----------

```bash
clojure -X:test                       # the tests on the JVM
npm install                           # once, for ClojureScript
clojure -M:cljs compile test          # the tests in Node
clojure -M:cljs compile browser-test  # the tests in a browser
node test/browser-run.mjs             # run them in a headless Chrome
```

On the JVM, a test also runs a subscriber, a publisher and a hub against
each other over HTTP on localhost.

License
-------

The websub-clj project is licensed under the [MIT licence](LICENSE).
