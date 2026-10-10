(ns dk.simongray.websub.test-setup
  "Give Node a DOMParser, so that qname-hiccup can read feeds outside a
  browser. The one from @xmldom/xmldom is installed under that name when
  the platform has none, configured to throw on every error the way
  browsers report a parsererror. In a browser this namespace does nothing."
  (:require ["@xmldom/xmldom" :as xmldom]))

(defn- throwing-parser
  "An xmldom DOMParser that throws on errors instead of logging them."
  []
  (xmldom/DOMParser. #js {:onError (fn [level message _context]
                                     (when-not (= level "warning")
                                       (throw (js/Error. message))))}))

(when-not (exists? js/DOMParser)
  (set! (.-DOMParser js/globalThis) throwing-parser))
