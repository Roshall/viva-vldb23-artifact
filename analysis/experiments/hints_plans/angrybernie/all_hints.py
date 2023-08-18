from viva.utils.config import ConfigManager
config = ConfigManager()
proxy_confidence_thresh = config.get_value('execution', 'proxy_confidence_thresh')

from viva.hints.superset_hint import SupersetHint
from viva.hints.equals_hint import EqualsHint
from viva.hints.proxy_hint import ProxyHint

from viva.nodes.filters import explode_preds, quality_filter
from viva.plans.plan_filters import two_people, faces_and, object_filter, similarity_filter

BernieHints = {
    'equals': [
        EqualsHint('odx', 'odl'),
        EqualsHint('odl', 'odm'),
        EqualsHint('odm', 'od'),
        EqualsHint('od', 'odn'),
        EqualsHint('ed', 'edc'),
        EqualsHint('edc', 'dfemo'),
        EqualsHint('fd', 'dfver')
    ],
    'supersets': [
        SupersetHint('si', 'odx')
    ],
    'proxys': [
        ProxyHint('ted', 'ed', proxy_confidence_thresh),
        ProxyHint('tfd', 'fd', proxy_confidence_thresh),
        ProxyHint('tod', 'odx', proxy_confidence_thresh),
        ProxyHint('todx', 'odx', proxy_confidence_thresh),
        ProxyHint('todl', 'odx', proxy_confidence_thresh),
        ProxyHint('todm', 'odx', proxy_confidence_thresh),
        ProxyHint('todn', 'odx', proxy_confidence_thresh),
        ProxyHint('tdfe', 'ed', proxy_confidence_thresh),
        ProxyHint('tdfv', 'fd', proxy_confidence_thresh),
    ]
}

# Only add in here if a superset or proxy
BernieHintFilters = {
    'tod' : [explode_preds, two_people],
    'todl': [explode_preds, two_people],
    'todx': [explode_preds, two_people],
    'todm': [explode_preds, two_people],
    'todn': [explode_preds, two_people],
    'ted' : [explode_preds, (object_filter, ['angry'])],
    'tdfe': [explode_preds, (object_filter, ['angry'])],
    'tfd' : [explode_preds, faces_and],
    'tdfv': [explode_preds, faces_and],
    'si'  : [(similarity_filter, [120])],
}
