(ns ^:no-doc dk.simongray.websub.document
  "The documents of a topic: the links that a page or a feed names for
  itself, and the entries of a feed, which a hub leaves out for a
  subscriber that has had them."
  (:require [clojure.string :as str]
            [dk.simongray.html-pieces :as html]
            [dk.simongray.qname-hiccup :as xml]
            [dk.simongray.wary-fetch.bytes :as bytes]
            [dk.simongray.wary-fetch.headers :as headers]
            [dk.simongray.wary-fetch.url :as url]))

(def atom-uri
  "The namespace of Atom."
  "http://www.w3.org/2005/Atom")

(def head-tags
  "The elements that a browser puts in the head of a page until the body
  begins, even in a page without a head tag."
  ;; HTML Standard, section 13.2.6.4.4, the "in head" insertion mode
  #{:base :basefont :bgsound :link :meta :noframes :noscript :script :style
    :template :title})

(defn- tag
  "The tag of `node`, or nil for text."
  [node]
  (when (vector? node)
    (first node)))

(defn absolute
  "The `href` made absolute against `base`, or as it is without a base."
  [base href]
  (if base
    (url/absolute-url base href)
    href))

(defn- unwrapped
  "The `nodes` with the children of each element of `t` in its place."
  [t nodes]
  (mapcat #(if (= t (tag %)) (drop 2 %) [%]) nodes))

(defn- head
  "The elements in the head of the page with the `nodes`, where a browser
  puts them: those of its head element and those before its first
  content."
  [nodes]
  (->> nodes
       (unwrapped :html)
       (unwrapped :head)
       (remove #(and (string? %) (str/blank? %)))
       (take-while #(head-tags (tag %)))))

(defn- elements
  "The elements of the page with the `nodes`, at any depth."
  [nodes]
  (filter vector? (mapcat #(tree-seq vector? (partial drop 2) %) nodes)))

;; HTML Standard, sections 4.2.3 and 4.2.4; WebSub 8.1: the head alone,
;; unless `body?`
(defn- page-links
  "The links in the head of the HTML page `s` at `url`, or in all of it
  when `body?`, made absolute against its base."
  [s url body?]
  (let [nodes (html/parse s)
        head  (head nodes)
        attrs (fn [t xs]
                (keep #(when (= t (tag %)) (second %)) xs))
        base  (or (some->> (attrs :base head)
                           (keep :href)
                           first
                           (absolute url))
                  url)]
    (for [{:keys [rel href]} (attrs :link (if body? (elements nodes) head))
          :when              href]
      {:rel rel :href (absolute base href)})))

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
  "The `body` of a response as text, decoded by its `charset` when it's
  bytes."
  [body charset]
  (if (string? body)
    body
    (some-> body (xml/decode charset))))

;; RFC 9110, section 8.3: a document without a media type can be examined
(defn kind
  "The kind of the document `s` of `media-type`, :html or :xml, or nil for
  another. Without a media type, its start tells."
  [media-type s]
  (cond
    (#{"text/html" "application/xhtml+xml"} media-type)
    :html

    (some->> media-type (re-find #"[/+]xml$"))
    :xml

    (or media-type (not (string? s)))
    nil

    (re-find #"(?i)^\s*<(!doctype html|html)" s)
    :html

    (re-find #"^\s*<" s)
    :xml))

(defn links
  "The links that the document in the `response` names for itself, as maps
  of :rel and :href, by `opts`: those in the head of an HTML page, or in
  all of it by :body-links?, or the atom:links of an Atom or RSS feed. Nil
  for another document, or one that doesn't parse."
  [{:keys [url headers body] :as response} {:keys [body-links?] :as opts}]
  (let [type (headers/media-type (headers/header headers "content-type"))
        s    (text body (headers/charset headers))]
    (try
      (case (kind type s)
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
