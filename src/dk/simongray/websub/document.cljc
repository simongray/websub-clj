(ns ^:no-doc dk.simongray.websub.document
  "The documents of a topic: the links that a page or a feed names for
  itself, and the entries of a feed, which a hub leaves out for a
  subscriber that has had them."
  (:require [clojure.string :as str]
            [dk.simongray.html-pieces :as html]
            [dk.simongray.qname-hiccup :as xml]
            [dk.simongray.wary-fetch.bytes :as bytes]
            [dk.simongray.whatwg-mime :as mime]
            [dk.simongray.whatwg-mime.patterns :as patterns]
            [dk.simongray.whatwg-url :as url]))

(def atom-uri
  "The namespace of Atom."
  "http://www.w3.org/2005/Atom")

(defn- tag
  "The tag of `node`, or nil for text."
  [node]
  (when (vector? node)
    (first node)))

;; URL Standard, as a browser resolves the links of a page
(defn absolute
  "The `href` made absolute against `base`, or as it is without a base. Nil
  when it doesn't resolve against the base."
  [base href]
  (if base
    (url/resolve base href)
    href))

;; WebSub 8.1: the head alone, unless `body?`
(defn- page-links
  "The links in the head of the HTML page `s` at `url`, or in all of it
  when `body?`, made absolute against its base."
  [s url body?]
  (for [[_ {:keys [rel href]}] (html/links s {:base-url  url
                                             :link-tags #{:link}
                                             :body?     (boolean body?)})]
    {:rel rel :href href}))

(defn- rebased
  "The `base` of the `element`'s content, by its xml:base."
  [base element]
  (if-let [b (:xml/base (xml/attrs element))]
    (absolute base b)
    base))

;; WebSub 4 and RFC 8288, appendix B; RFC 4287, section 2: xml:base
(defn- feed-links
  "The atom:links of the feed `s` at `url` itself, not those of its entries,
  made absolute against their xml:base."
  [s url]
  (let [root      (xml/parse s {atom-uri :atom})
        channel   (some #(when (= "channel" (name (tag %))) %)
                        (xml/elements root))
        root-base (rebased url root)
        parents   [[root root-base]
                   [channel (some->> channel (rebased root-base))]]]
    (for [[parent base] parents
          :when         parent
          link          (xml/elements parent :atom/link)
          :let          [{:keys [rel href]} (xml/attrs link)]
          :when         href]
      {:rel rel :href (absolute (rebased base link) href)})))

(defn- text
  "The `body` of a response as text, decoded by its `encoding` when it's
  bytes."
  [body encoding]
  (if (string? body)
    body
    (some-> body (xml/decode encoding))))

(defn- start
  "The first bytes of the `body` of a response, in UTF-8 when it's text."
  [body]
  (if (string? body)
    (bytes/encode (subs body 0 (min (count body) patterns/resource-header-length)))
    body))

;; MIME Sniffing, section 7, with its rules until 2024 for a feed served as
;; HTML, since a feed names its hubs whatever its type
(defn kind
  "The kind of the document whose first bytes are `b`, of the MIME type
  `m`: :html, :xml or nil for another."
  [b m]
  (let [computed (mime/sniff b m)]
    (cond
      (patterns/feed-type b)
      :xml

      (#{"text/html" "application/xhtml+xml"} (mime/essence computed))
      :html

      (mime/xml? computed)
      :xml)))

(defn links
  "The links that the document in the `response` names for itself, as maps
  of :rel and :href, by `opts`: those in the head of an HTML page, or in
  all of it by :body-links?, or the atom:links of an Atom or RSS feed. Nil
  for another document, or one that doesn't parse."
  [{:keys [url headers body] :as response} {:keys [body-links?] :as opts}]
  (let [type (mime/extract (get headers "content-type"))
        s    (text body (mime/encoding type))]
    (try
      (case (kind (start body) type)
        :html (page-links s url body-links?)
        :xml  (feed-links s url)
        nil)
      (catch #?(:clj Exception :cljs :default) _
        nil))))

(defn- entries-parent
  "The element of the feed `tree` that holds its entries: the root of an
  Atom feed, or the channel of RSS 2.0. Nil for another document."
  [tree]
  (case (tag tree)
    :atom/feed tree
    :rss       (xml/element tree :channel)
    nil))

(defn- entry?
  "Whether `node` is an entry of an Atom or RSS 2.0 feed."
  [node]
  (contains? #{:atom/entry :item} (tag node)))

;; RFC 4287, section 4.2.6; RSS 2.0, the guid of an item
(defn- entry-id
  "The id of the feed `entry`: the atom:id of an Atom entry, or the guid of
  an RSS item or else its link. Nil when it has none."
  [entry]
  (let [tags (if (= :atom/entry (tag entry)) [:atom/id] [:guid :link])]
    (some #(some-> (xml/element entry %) xml/text xml/trim not-empty)
          tags)))

(defn- without
  "The `element` without the entries whose ids are in `delivered`."
  [element delivered]
  (into (subvec element 0 2)
        (remove #(and (entry? %) (contains? delivered (entry-id %))))
        (xml/children element)))

;; WebSub 7: a hub may leave out the atom:entry and rss:item elements that
;; it delivered before
(defn diff
  "The feed in the bytes `body` without the entries whose ids are in
  `delivered`, given the `charset` of its Content-Type, as a map of the
  :body in UTF-8 and the :ids of all its entries. Nil for a body that is
  no Atom or RSS 2.0 feed, or whose charset is another than UTF-8."
  [body charset delivered]
  (when (contains? #{nil "utf-8"} (some-> charset str/lower-case))
    (try
      (let [tree   (xml/parse (xml/decode body) {atom-uri :atom})
            parent (entries-parent tree)
            ids    (->> (some-> parent xml/elements)
                        (filter entry?)
                        (keep entry-id)
                        set)
            uris   (merge {:atom atom-uri} (xml/namespaces tree))
            fewer  (if (= parent tree)
                     (without tree delivered)
                     (into (subvec tree 0 2)
                           (map #(if (identical? parent %)
                                   (without % delivered)
                                   %))
                           (xml/children tree)))]
        (when parent
          {:body (bytes/encode (xml/emit fewer uris))
           :ids  ids}))
      (catch #?(:clj Exception :cljs :default) _
        nil))))
