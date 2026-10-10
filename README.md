websub-clj
==========

This is a Clojure and ClojureScript implementation of
[WebSub](https://www.w3.org/TR/websub/), the W3C standard by which a hub
pushes a feed or a page to its subscribers as soon as it changes, so that
they don't have to poll it. It covers all three roles of the standard:
the subscriber, the publisher and the hub.

- **The whole standard.** The code follows the Recommendation of 2 June
  2026, and cites the section of each rule it implements. The scenarios
  of the [websub.rocks](https://websub.rocks) test suite are among its
  tests.
- **Plain data.** Requests are maps that any HTTP client can send, and
  the callback of a subscriber and the hub are Ring handlers.
- **Nothing in the background.** The hub gives its work to you as jobs,
  plain maps, and you run them when and where you like.
- **Safe by default.** A secret only goes over https, signatures are
  compared in constant time, and the hub only sends to public addresses,
  never to its own network.

The same code runs on the JVM, in Node and in the browser.

> This library was spun out of [podcast-clj](https://github.com/simongray/podcast-clj),
> a library for making podcast software, where it lets a server have the
> feeds it follows pushed to it. Like podcast-clj, it was developed with
> assistance from frontier LLMs.

Getting started
---------------

It requires Clojure 1.11+ and Java 11+. Add it to the `:deps` in your
`deps.edn` as a Git dependency, with the SHA of the latest commit on
`master`:

```clojure
dk.simongray/websub-clj {:git/url "https://github.com/simongray/websub-clj"
                         :git/sha "…"}
```

For ClojureScript, also set `:deps true` in your `shadow-cljs.edn`, since
shadow-cljs only reads Git dependencies from `deps.edn`. To read feeds in
Node, install a DOMParser as `globalThis.DOMParser`, e.g. that of
`@xmldom/xmldom`.

### Subscribe

Find the hubs of a topic with `discover!`:

```clojure
(require '[dk.simongray.websub :as websub])

(websub/discover! "https://simon.grays.blog/feed")
;; => {:status 200
;;     :hubs   ["https://pubsubhubbub.superfeedr.com/"]
;;     :topic  "https://simon.grays.blog/feed"}
```

If you fetch the topic yourself, `discover` reads the same from the
response. Then ask a hub for the topic, giving it a callback URL that
nobody can guess and a secret to sign what it sends:

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

The `:wanted-pred` says whether you asked for the subscription of the
`:topic`, and `:deliver!` gets the bytes of each delivery that has the
signature of the secret. Return from it quickly, and read the feed
afterwards.

The check also gives `:wanted-pred` the `:lease-seconds` of the
subscription. Before the lease ends, at `websub/next-renewal`, discover
the topic again, since it may have moved, and subscribe again with a new
callback URL.

### Publish

Name the hub in a Link header of the topic, or in its document with the
maps of `websub/links`, and tell the hub each time the topic changes:

```clojure
(websub/link-header ["https://pubsubhubbub.superfeedr.com/"]
                    "https://simon.grays.blog/feed")
;; => "<https://pubsubhubbub.superfeedr.com/>; rel=\"hub\", <https://simon.grays.blog/feed>; rel=\"self\""

(websub/publish! ["https://pubsubhubbub.superfeedr.com/"]
                 "https://simon.grays.blog/feed")
;; => [{:hub "https://pubsubhubbub.superfeedr.com/" :status 204}]
```

### Run a hub

A hub is a Ring handler, a store of subscriptions and a function of
yours that runs jobs, e.g. on a thread pool or from a queue in a
database:

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

The handler answers subscribers and publishers at once, and gives the
work that follows to `schedule!`: a check of a subscription, or the
delivery of a topic that changed. A delivery that fails comes back as a
retry with an `:at`. A store is a map of two functions, so one over a
database is a few lines, and `hub/default-options` lists the rest of the
options, e.g. the leases and the retries.

Principles
----------

- **Standards first.** The code follows the Recommendation and cites its
  sections, and says so where it makes a choice of its own.
- **Plain data.** Requests, jobs and reports are maps, and every
  function that sends takes a `:send` of your own.
- **One codebase.** The library is written in `.cljc`, and depends on
  [wary-fetch](https://github.com/simongray/wary-fetch) for requests,
  [qname-hiccup](https://github.com/simongray/qname-hiccup) for feeds and
  [html-pieces](https://github.com/simongray/html-pieces) for pages.

Development
-----------

```bash
clojure -X:test                       # the tests on the JVM
npm install                           # once, for the Node tests
clojure -M:cljs compile test          # the tests in Node
clojure -M:cljs compile browser-test  # for a browser, from target/browser-test
```

On the JVM, a test also runs a subscriber, a publisher and a hub against
each other over HTTP on localhost.

License
-------

The websub-clj project is licensed under the [MIT licence](LICENSE).
