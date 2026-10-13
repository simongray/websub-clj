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

The same code runs on the JVM, in Node and in the browser.

> This library was spun out of [podcast-clj](https://github.com/simongray/podcast-clj),
> a library for making podcast software, where servers use it to get new
> episodes pushed to them instead of polling every feed. Like podcast-clj,
> it was developed with assistance from frontier LLMs.

Getting started
---------------

It requires Clojure 1.11+ and Java 11+. For the latest release, add it
from [Clojars](https://clojars.org/dk.simongray/websub-clj) to the
`:deps` in your `deps.edn`:

```clojure
dk.simongray/websub-clj {:mvn/version "0.6.0"}
```

For changes that aren't released yet, use the SHA of the latest commit on
`master` instead:

```clojure
dk.simongray/websub-clj
{:git/url "https://github.com/simongray/websub-clj"
 :git/sha "…"}
```

For ClojureScript, shadow-cljs only reads Git dependencies from
`deps.edn`, so also set `:deps true` in your `shadow-cljs.edn`. To read
feeds in Node, set `globalThis.DOMParser` to a DOMParser, e.g. the one
from `@xmldom/xmldom`.

### Subscribe

Find the hubs of a topic with `discover!`:

```clojure
(require '[dk.simongray.websub :as websub])

(websub/discover! "https://simon.grays.blog/feed")
;; => {:status 200
;;     :hubs   ["https://pubsubhubbub.superfeedr.com/"]
;;     :topic  "https://simon.grays.blog/feed"}
```

If you fetch the topic yourself, give the response to `discover` for the
same map. A feed that names a hub but no `rel="self"` link gives the hubs
with a nil `:topic`, since WebSub takes the topic from that link. Many
RSS feeds leave it out, so give `{:url-as-topic? true}` to take the URL
that you fetched as the topic instead. Then subscribe to the topic at a
hub, giving it a callback URL that nobody can guess and a secret to sign
what it sends:

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

A secret goes over https alone, so a hub with an http URL gets the
request at its https URL. If that fails, `call!` throws
`:dk.simongray.websub/https-failed`, and `{:secret-over-http? true}`
sends it over http instead.

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

The `:wanted-pred` says whether you asked for a subscription to the
`:topic`, and `:deliver!` gets the bytes of each delivery signed with the
secret. Return from it quickly, and read the feed afterwards.

Once `:wanted-pred` says yes, the handler confirms the subscription to
the hub and calls `:verified!`, if you give one, with the `:mode`, the
`:topic` and the `:lease-seconds` of the subscription. Keep the lease
there, and before it ends, at the time that `websub/next-renewal` gives,
subscribe again with a new callback URL. Discover the topic first, since
it may have moved.

### Publish

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

### Run a hub

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

The handler answers subscribers and publishers right away, and gives the
work that follows to `schedule!`: a check of a subscription, or the
delivery of a topic that changed. A delivery that fails goes to
`schedule!` again as a retry, with the `:at` to run it. A store is a map
of two functions, so one for a database takes a few lines. The options
that have defaults, e.g. the leases and the retries, are in
`hub/default-options`, and the docstrings of `hub/handler` and
`hub/run-job!` describe the others.

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
