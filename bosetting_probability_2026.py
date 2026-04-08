#!/usr/bin/env python3
"""
Calculate estimated settlement probability by municipality using IMDi 2026 request figures.

Input source:
https://www.imdi.no/bosetting/bosettingstall/nokkeltall-bosetting-2026/

The script extracts:
- Kommune
- value from column "Antall personer kommunen har vedtatt å bosette:"

and writes a text file with probabilities:
    kommune - percentage_of_total (%)
"""

from __future__ import annotations

import argparse
import html
import json
import urllib.error
import re
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import List, Optional


URL_DEFAULT = "https://www.imdi.no/bosetting/bosettingstall/nokkeltall-bosetting-2026/"
NOMINATIM_BASE_URL = "https://nominatim.openstreetmap.org"
NOMINATIM_SEARCH_URL = f"{NOMINATIM_BASE_URL}/search"
NOMINATIM_REVERSE_URL = f"{NOMINATIM_BASE_URL}/reverse"
OPEN_METEO_GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
PROVIDER_USER_AGENT = "bosetting-probability-script/1.0"
NORWAY_BOUNDS = (57.2, 4.0, 72.2, 32.5)
NOMINATIM_VIEWBOX = "4.0,57.8,32.0,72.2"
DEFAULT_REVERSE_ZOOM = 10
WATER_REVERSE_TYPES = {"water", "bay", "fjord", "sea", "ocean", "coastline"}
STRICT_COORDINATES_PATH_DEFAULT = "kommuner_strict_municipality_cache_2026.json"
DEFAULT_MAP_MIN_ZOOM = 4
DEFAULT_MAP_MAX_ZOOM = 12
DEFAULT_MARKER_CLUSTER_RADIUS = 50

# Real administrative-center coordinates for every Norwegian municipality (2024 borders).
# Key = municipality name as it appears in IMDi tables.
# Duplicate names carry a qualifier in parentheses matching the parsed label.
MUNICIPALITY_COORDS: dict[str, tuple[float, float]] = {
    # Oslo
    "Oslo": (59.9139, 10.7522),
    # Akershus
    "Bærum": (59.9117, 10.5252),
    "Lillestrøm": (59.9558, 11.0493),
    "Nittedal": (59.9724, 10.8533),
    "Frogn": (59.6731, 10.6671),
    "Nordre Follo": (59.7151, 10.8300),
    "Ullensaker": (60.1321, 11.1708),
    "Vestby": (59.6039, 10.7537),
    "Enebakk": (59.7403, 11.1488),
    "Nes": (60.1202, 11.4631),
    "Nesodden": (59.8581, 10.6569),
    "Eidsvoll": (60.3229, 11.2625),
    "Jevnaker": (60.2364, 10.3777),
    "Hurdal": (60.4047, 11.0710),
    "Nannestad": (60.2218, 11.0920),
    "Gjerdrum": (60.0757, 11.0287),
    "Lørenskog": (59.8866, 10.9635),
    "Ås": (59.6660, 10.7895),
    # Østfold
    "Indre Østfold": (59.6133, 11.3883),
    "Moss": (59.4323, 10.6591),
    "Våler": (59.4862, 10.8501),
    "Råde": (59.3497, 10.8373),
    "Aremark": (59.2319, 11.6870),
    "Sarpsborg": (59.2833, 11.1097),
    "Halden": (59.1265, 11.3871),
    "Skiptvet": (59.5683, 11.2530),
    "Hvaler": (59.0338, 10.9997),
    "Rakkestad": (59.3782, 11.3458),
    "Fredrikstad": (59.2181, 10.9298),
    "Marker": (59.4164, 11.6308),
    # Buskerud
    "Kongsberg": (59.6627, 9.6529),
    "Ringerike": (60.1707, 10.2596),
    "Drammen": (59.7441, 10.2045),
    "Modum": (59.9796, 10.0100),
    "Hol": (60.6235, 8.3058),
    "Sigdal": (60.0892, 9.7506),
    "Øvre Eiker": (59.7714, 9.9083),
    "Nore og Uvdal": (60.3602, 8.9758),
    "Lier": (59.7954, 10.2540),
    "Gol": (60.7007, 8.9456),
    "Rollag": (59.9568, 9.2536),
    "Ål": (60.6274, 8.5665),
    "Flesberg": (59.8309, 9.5877),
    "Hemsedal": (60.8605, 8.5603),
    "Nesbyen": (60.5695, 9.1122),
    "Krødsherad": (60.1709, 9.5797),
    "Hole": (60.1059, 10.2497),
    "Flå": (60.3766, 9.4984),
    # Vestfold
    "Tønsberg": (59.2671, 10.4076),
    "Larvik": (59.0530, 10.0337),
    "Horten": (59.4169, 10.4858),
    "Færder": (59.2127, 10.4065),
    "Holmestrand": (59.4886, 10.3147),
    # Telemark
    "Bamble": (59.0108, 9.6055),
    "Skien": (59.2098, 9.6066),
    "Porsgrunn": (59.1427, 9.6563),
    "Tinn": (59.8741, 8.7694),
    "Fyresdal": (59.1823, 8.0903),
    "Notodden": (59.5581, 9.2643),
    "Kragerø": (58.8669, 9.4083),
    "Hjartdal": (59.5939, 8.7889),
    "Nissedal": (59.1895, 8.4747),
    "Seljord": (59.3848, 8.8308),
    "Vinje": (59.5655, 7.9730),
    "Tokke": (59.3780, 8.0670),
    "Drangedal": (59.0917, 9.0700),
    "Midt-Telemark": (59.3857, 9.0821),
    # Agder
    "Kristiansand": (58.1599, 7.9956),
    "Arendal": (58.4617, 8.7669),
    "Grimstad": (58.3405, 8.5931),
    "Lindesnes": (58.0290, 7.4610),
    "Vennesla": (58.2853, 7.9606),
    "Risør": (58.7218, 9.2278),
    "Farsund": (58.0944, 6.8048),
    "Kvinesdal": (58.3040, 6.8949),
    "Flekkefjord": (58.2962, 6.6607),
    "Vegårshei": (58.7223, 8.7823),
    "Lillesand": (58.2494, 8.3775),
    "Sirdal": (58.7618, 6.7169),
    "Tvedestrand": (58.6167, 8.9328),
    "Hægebostad": (58.4828, 7.1972),
    "Iveland": (58.4459, 8.0527),
    "Valle": (59.2021, 7.5306),
    "Åmli": (58.8213, 8.4831),
    "Åseral": (58.4909, 7.3695),
    "Birkenes": (58.3452, 8.2305),
    "Bykle": (59.5684, 7.3828),
    "Evje og Hornnes": (58.5865, 7.8033),
    "Lyngdal": (58.1408, 7.0730),
    # Rogaland
    "Stavanger": (58.9700, 5.7331),
    "Sandnes": (58.8521, 5.7351),
    "Haugesund": (59.4138, 5.2680),
    "Gjesdal": (58.8272, 5.9372),
    "Karmøy": (59.2823, 5.2989),
    "Vindafjord": (59.5112, 5.8538),
    "Hå": (58.6111, 5.6508),
    "Klepp": (58.7671, 5.6348),
    "Time": (58.7261, 5.7711),
    "Tysvær": (59.3780, 5.5046),
    "Randaberg": (59.0021, 5.6170),
    "Sauda": (59.6475, 6.3504),
    "Suldal": (59.4886, 6.2510),
    "Eigersund": (58.4541, 6.0002),
    "Strand": (59.0265, 6.0462),
    "Bjerkreim": (58.6612, 6.0410),
    "Hjelmeland": (59.2263, 6.1564),
    "Lund": (58.3595, 6.0525),
    "Bokn": (59.2341, 5.4464),
    "Sokndal": (58.3399, 6.2367),
    # Vestland
    "Bergen": (60.3913, 5.3221),
    "Alver": (60.5878, 5.1194),
    "Kinn": (61.5988, 5.0199),
    "Øygarden": (60.3906, 5.0847),
    "Askøy": (60.3989, 5.1882),
    "Bømlo": (59.7756, 5.2083),
    "Luster": (61.4484, 7.4530),
    "Kvinnherad": (59.8750, 6.2643),
    "Sogndal": (61.2298, 7.0942),
    "Sunnfjord": (61.4526, 5.8433),
    "Voss": (60.6290, 6.4118),
    "Kvam": (60.3846, 6.2239),
    "Stryn": (61.9050, 6.7227),
    "Bjørnafjorden": (60.0635, 5.6246),
    "Gloppen": (61.7756, 6.2189),
    "Høyanger": (61.2182, 6.0726),
    "Osterøy": (60.5126, 5.5236),
    "Stad": (62.0490, 5.2316),
    "Sveio": (59.6195, 5.3594),
    "Lærdal": (61.0996, 7.4821),
    "Tysnes": (59.9956, 5.5155),
    "Vaksdal": (60.4796, 5.7585),
    "Vik": (61.0864, 6.5769),
    "Austevoll": (60.0862, 5.2555),
    "Årdal": (61.2382, 7.6936),
    "Aurland": (60.9050, 7.1850),
    "Austrheim": (60.7614, 4.9560),
    "Fjaler": (61.3685, 5.7124),
    "Hyllestad": (61.2177, 5.3032),
    "Masfjorden": (60.8316, 5.3927),
    "Solund": (61.0700, 4.8379),
    "Stord": (59.7791, 5.4915),
    "Eidfjord": (60.4666, 7.0700),
    "Samnanger": (60.3966, 5.7762),
    "Fitjar": (59.8283, 5.3113),
    "Ullensvang": (60.1308, 6.6463),
    "Ulvik": (60.5668, 6.9193),
    # Møre og Romsdal
    "Ålesund": (62.4723, 6.1549),
    "Kristiansund": (63.1102, 7.7278),
    "Hareid": (62.3731, 6.0278),
    "Hustadvika": (63.0072, 7.0972),
    "Ulstein": (62.3422, 5.9638),
    "Averøy": (63.0492, 7.5264),
    "Volda": (62.1460, 6.0695),
    "Haram": (62.5389, 6.2055),
    "Sunndal": (62.6736, 8.5647),
    "Ørsta": (62.1982, 6.1288),
    "Stranda": (62.3099, 6.9413),
    "Aure": (63.2948, 8.3364),
    "Fjord": (62.2513, 7.0637),
    "Giske": (62.4850, 6.0333),
    "Gjemnes": (62.9498, 7.6437),
    "Rauma": (62.5676, 7.6916),
    "Surnadal": (62.9685, 8.7127),
    "Aukra": (62.8660, 6.5742),
    "Smøla": (63.3663, 7.7861),
    "Tingvoll": (63.0285, 8.2039),
    "Sande (Møre og Romsdal)": (62.2119, 5.7738),
    "Vanylven": (62.1488, 5.6985),
    # Innlandet
    "Gjøvik": (60.7957, 10.6914),
    "Kongsvinger": (60.1924, 12.0045),
    "Elverum": (60.8811, 11.5625),
    "Søndre Land": (60.6974, 10.2461),
    "Vestre Toten": (60.6546, 10.5574),
    "Østre Toten": (60.7257, 10.7081),
    "Gran": (60.3767, 10.5574),
    "Nordre Land": (60.8801, 10.1753),
    "Ringsaker": (60.8900, 10.7941),
    "Stange": (60.7088, 11.1669),
    "Hamar": (60.7945, 11.0678),
    "Lillehammer": (61.1153, 10.4662),
    "Sør-Odal": (60.2250, 11.6005),
    "Øystre Slidre": (61.1168, 9.0271),
    "Nord-Fron": (61.5845, 9.9534),
    "Rendalen": (61.2553, 11.1459),
    "Sel": (61.8652, 10.0127),
    "Tynset": (62.2783, 10.7743),
    "Vågå": (61.8784, 9.1373),
    "Nord-Aurdal": (60.8889, 9.3102),
    "Ringebu": (61.5158, 10.0638),
    "Sør-Fron": (61.5592, 10.0142),
    "Vestre Slidre": (61.0868, 8.9919),
    "Åmot": (61.1267, 11.5047),
    "Folldal": (62.1329, 10.0022),
    "Os (Hedm.)": (62.4831, 11.2239),
    "Tolga": (62.4181, 11.0382),
    "Trysil": (61.3152, 12.2656),
    "Alvdal": (62.1097, 10.6328),
    "Dovre": (62.0706, 9.6860),
    "Etnedal": (60.8800, 9.6854),
    "Gausdal": (61.2468, 10.1800),
    "Lesja": (62.2289, 8.8691),
    "Løten": (60.8145, 11.3480),
    "Skjåk": (61.9054, 8.2289),
    "Stor-Elvdal": (61.3939, 11.0549),
    "Sør-Aurdal": (60.6358, 9.7932),
    "Øyer": (61.2615, 10.4212),
    "Lom": (61.8369, 8.5713),
    "Vang": (61.1254, 8.5582),
    "Nord-Odal": (60.3896, 11.5616),
    "Våler (Innlandet)": (60.6432, 11.9326),
    "Åsnes": (60.7433, 11.8745),
    # Trøndelag
    "Trondheim": (63.4305, 10.3951),
    "Levanger": (63.7468, 11.2992),
    "Verdal": (63.7925, 11.4843),
    "Heim": (63.3464, 9.2700),
    "Oppdal": (62.5933, 9.6907),
    "Steinkjer": (64.0150, 11.4953),
    "Stjørdal": (63.4690, 10.9170),
    "Namsos": (64.4667, 11.4961),
    "Orkland": (63.2989, 9.8499),
    "Røros": (62.5744, 11.3848),
    "Hitra": (63.5740, 8.8350),
    "Ørland": (63.7009, 9.6152),
    "Malvik": (63.4270, 10.6785),
    "Åfjord": (63.9755, 10.2311),
    "Inderøy": (63.8694, 11.2803),
    "Meråker": (63.4151, 11.7458),
    "Melhus": (63.2899, 10.2844),
    "Skaun": (63.2790, 10.2890),
    "Midtre Gauldal": (63.1543, 10.6546),
    "Overhalla": (64.4694, 11.9656),
    "Grong": (64.4591, 12.3083),
    "Frosta": (63.5929, 10.6940),
    "Frøya": (63.7282, 8.6520),
    "Lierne": (64.4736, 13.8194),
    "Rindal": (63.0436, 9.2122),
    "Selbu": (63.2086, 11.0364),
    "Rennebu": (62.8298, 10.0084),
    "Holtålen": (62.8020, 11.2830),
    "Indre Fosen": (63.5843, 10.2230),
    "Namsskogan": (64.9326, 13.1646),
    "Nærøysund": (64.8695, 11.4028),
    "Osen": (64.4244, 10.7032),
    "Flatanger": (64.4860, 10.9700),
    # Nordland
    "Bodø": (67.2804, 14.4049),
    "Narvik": (68.4385, 17.4270),
    "Alstahaug": (65.8806, 12.4948),
    "Vefsn": (65.8347, 13.1931),
    "Vågan": (68.2338, 14.5691),
    "Vestvågøy": (68.1540, 13.9590),
    "Hadsel": (68.5596, 14.7929),
    "Andøy": (69.1421, 15.7920),
    "Bø (Nordland)": (68.7531, 14.4654),
    "Gildeskål": (67.0764, 14.7277),
    "Øksnes": (68.7230, 15.0080),
    "Fauske": (67.2598, 15.3920),
    "Dønna": (66.0616, 12.3835),
    "Evenes": (68.5339, 16.5452),
    "Hemnes": (66.1944, 13.8965),
    "Lødingen": (68.4110, 16.0006),
    "Saltdal": (66.8460, 15.3840),
    "Steigen": (67.8073, 15.0530),
    "Sømna": (65.2862, 12.1226),
    "Sortland": (68.6930, 15.4133),
    "Brønnøy": (65.4624, 12.2095),
    "Hamarøy": (68.0613, 15.5224),
    "Lurøy": (66.4037, 12.8936),
    "Meløy": (66.8601, 13.7610),
    "Beiarn": (67.0091, 15.5459),
    "Bindal": (65.0646, 12.3239),
    "Grane": (65.4437, 13.5263),
    "Hattfjelldal": (65.5677, 14.0606),
    "Herøy (Nordland)": (66.0167, 12.3028),
    # Troms
    "Harstad": (68.7984, 16.5415),
    "Tromsø": (69.6496, 18.9560),
    "Senja": (69.0573, 17.1163),
    "Balsfjord": (69.2008, 19.1698),
    "Lyngen": (69.5685, 20.1949),
    "Målselv": (69.0252, 18.2688),
    "Nordreisa": (69.7628, 21.0330),
    "Sørreisa": (69.0001, 18.1915),
    "Salangen": (68.7956, 18.2437),
    "Skjervøy": (70.0333, 20.9833),
    "Tjeldsund": (68.6582, 16.5670),
    "Kvæfjord": (68.7802, 16.5371),
    "Kåfjord": (69.3997, 20.4744),
    "Lavangen": (68.7461, 18.0044),
    "Storfjord": (69.2006, 19.9048),
    "Dyrøy": (69.1112, 18.1260),
    # Finnmark
    "Alta": (69.9689, 23.2716),
    "Vadsø": (70.0743, 29.7509),
    "Sør-Varanger": (69.7252, 30.0469),
    "Hammerfest": (70.6634, 23.6821),
    "Nordkapp": (70.9787, 25.7839),
    "Tana": (70.0087, 27.0111),
    "Båtsfjord": (70.6342, 29.7124),
    "Karasjok": (69.4698, 25.5125),
    "Lebesby": (70.3827, 26.8500),
    "Loppa": (70.2385, 21.3459),
    "Porsanger": (70.0589, 25.0806),
}


@dataclass
class Record:
    county: str
    municipality: str
    accepted_to_settle: int


def clean_text(value: str) -> str:
    """Remove html tags, decode entities, normalize spaces."""
    value = html.unescape(value)
    value = repair_mojibake(value)
    value = re.sub(r"<[^>]+>", " ", value)
    value = value.replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def sanitize_municipality_name(value: str) -> str:
    value = clean_text(value)
    if not value:
        return ""
    value = re.sub(r"\s*\([^)]+\)\s*$", "", value).strip()
    value = re.sub(r"\s*[†*]\s*$", "", value)
    value = value.strip(" -–—,:;.")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def strict_municipality_cache_key(
    municipality: str,
    county: str | None = None,
) -> str:
    municipality_norm = normalize_label(municipality)
    if not municipality_norm:
        return ""

    county_norm = normalize_label(county or "")
    if county_norm and county_norm != "ukjent fylke":
        return f"{county_norm}|{municipality_norm}"
    return municipality_norm


def parse_strict_cache_key(cache_key: str) -> tuple[str, str]:
    if not isinstance(cache_key, str):
        return "", ""

    normalized = cache_key.strip()
    if not normalized:
        return "", ""

    if "|" not in normalized:
        return "", normalize_label(normalized)

    county_part, municipality_part = normalized.split("|", 1)
    return normalize_label(county_part), normalize_label(municipality_part)


def load_reference_municipality_names(path: str | None = None) -> set[str]:
    names = set(MUNICIPALITY_COORDS.keys())
    if not path:
        return names
    try:
        with open(path, "r", encoding="utf-8") as cache_file:
            data = json.load(cache_file)
        if isinstance(data, dict):
            for key, value in data.items():
                if not isinstance(key, str) or not key.strip() or not isinstance(value, (list, tuple)):
                    continue
                if len(value) != 2:
                    continue
                _, municipality_name = parse_strict_cache_key(key)
                municipality_name = sanitize_municipality_name(municipality_name)
                if municipality_name:
                    names.add(municipality_name)
    except FileNotFoundError:
        pass
    except Exception as exc:
        print(
            f"WARNING: cannot load municipality reference list from {path}: {exc}",
            file=sys.stderr,
        )
    return names


def is_reference_municipality(name: str, reference_names: Optional[set[str]] = None) -> bool:
    if reference_names is None:
        return True
    if not name:
        return False

    direct = sanitize_municipality_name(name)
    if direct in reference_names:
        return True

    if not reference_names:
        return True

    direct_norm = normalize_label(direct)
    normalized_reference = {normalize_label(m) for m in reference_names}
    if direct_norm in normalized_reference:
        return True

    if not direct_norm:
        return False

    if reference_names:
        for candidate in (
            sanitize_municipality_name(direct.split("-", 1)[0]),
            sanitize_municipality_name(direct.split(" - ", 1)[0]),
            sanitize_municipality_name(direct.split("–", 1)[0]),
            sanitize_municipality_name(direct.split("—", 1)[0]),
        ):
            if not candidate:
                continue
            if candidate in reference_names or normalize_label(candidate) in normalized_reference:
                return True

    return False


def parse_int_cell(value: str) -> Optional[int]:
    """
    Parse integer from a table cell.
    Non-numeric values (avventer vedtak, -, :) are treated as missing.
    """
    text = clean_text(value).strip().lower()
    if text in {"", "-", ":", ":", "avventer vedtak", "avventer", "n/a", "na"}:
        return None
    number_text = re.sub(r"[^0-9]", "", text)
    if not number_text:
        return None
    return int(number_text)


def repair_mojibake(value: str) -> str:
    """
    Fix common UTF-8 bytes that were decoded with latin1/cp1252.
    """
    if "Ã" not in value:
        return value
    try:
        repaired = value.encode("latin1", errors="ignore").decode("utf-8")
    except Exception:
        return value
    # Keep repair only if it looks better for Nordic characters.
    if any(ch in repaired for ch in "ØøÆæÅåÃ") and "Ã" not in repaired:
        return repaired
    # If still looks like mojibake, fallback.
    if "Ã" in repaired:
        return value
    return repaired


def parse_header_candidates(raw_header: str) -> List[str]:
    cleaned = clean_text(raw_header)
    # Normalize norwegian title noise that sometimes appears with different hyphens/spaces.
    normalized = cleaned.lower().replace("\u2013", "-").replace("\u2014", "-")
    return [segment.strip() for segment in re.split(r"\|", normalized)]


def extract_county_name_before_table(html_body: str, table_start: int) -> str:
    """
    Try to infer county name from the nearest previous section heading:
    "<h3>Agder - oversikt over bosettingen i 2026</h3>" -> "Agder".
    """
    heading_pattern = re.compile(r"(?is)<h[1-6][^>]*>(.*?)</h[1-6]>")
    matches = list(heading_pattern.finditer(html_body, 0, table_start))
    if not matches:
        return ""

    heading_text = clean_text(matches[-1].group(1))
    heading_lower = heading_text.lower()
    if "oversikt over bosettingen i 2026" not in heading_lower:
        return ""
    if "fylkene i 2026" in heading_lower:
        return ""
    if "anmodning 2026" in heading_lower:
        return ""

    county_heading_pattern = re.compile(
        r"^(?P<county>.+?)\s*[-\u2013\u2014]\s*oversikt over bosettingen i 2026$",
        re.IGNORECASE,
    )
    heading_match = county_heading_pattern.match(heading_text)
    if heading_match is None:
        return ""

    county = heading_match.group("county").strip()
    return county


def is_municipality_table(headers: List[str]) -> bool:
    h_join = " | ".join(headers).lower()
    if not headers:
        return False
    # The first column for municipality tables is exactly "Kommune".
    # Fylke-level tables should be excluded.
    first_header = headers[0].strip().lower()
    if not first_header.startswith("kommune"):
        return False
    if "har vedtatt" in h_join and "bosette" in h_join:
        return True
    # fallback for changed wording
    return "antall personer kommunen har vedtatt" in h_join


def find_target_column(headers: List[str]) -> Optional[int]:
    target_hints = [
        "antall personer kommunen har vedtatt a",
        "kommunen har vedtatt a",
        "antall personer kommunen har vedtatt",
        "har vedtatt",
    ]
    h_lower = [h.lower() for h in headers]
    for i, header in enumerate(h_lower):
        if "har vedtatt" in header and "bosette" in header:
            return i
    for needle in target_hints:
        for i, header in enumerate(h_lower):
            if needle in header:
                return i
    # fallback: second column in a kommune table
    if len(h_lower) > 1:
        return 1
    return None


def decode_web_content(raw: bytes, charset: Optional[str]) -> str:
    """
    Decode bytes with fallback encodings and a small mojibake recovery step.
    """
    candidates = []
    if charset:
        candidates.append(charset)
    candidates.extend(["utf-8", "iso-8859-1", "cp1252", "windows-1252"])

    last_error = None
    for enc in candidates:
        if not enc:
            continue
        try:
            decoded = raw.decode(enc, errors="strict")
            if "Ã" in decoded and "ø" not in decoded and "Ø" not in decoded and "æ" not in decoded:
                # Typical latinized UTF-8 bytes decoded as latin1/cp1252
                latin_repair = decoded.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
                if "Ã" not in latin_repair:
                    decoded = latin_repair
            return decoded
        except Exception as exc:  # pragma: no cover - compatibility fallback
            last_error = exc

    # Last resort: decode with latin1 to avoid hard failure.
    if last_error is not None:
        print(f"WARNING: decoding issues ({last_error}), fallback to latin1", file=sys.stderr)
    return raw.decode("latin1", errors="replace")


def parse_tables(
    html_body: str,
    include_zero: bool = False,
    reference_municipality_names: Optional[set[str]] = None,
    require_reference_match: bool = False,
) -> List[Record]:
    table_pattern = re.compile(r"(?is)<table\b[^>]*>(.*?)</table>")
    row_pattern = re.compile(r"(?is)<tr\b[^>]*>(.*?)</tr>")
    cell_pattern = re.compile(r"(?is)<t[hd]\b[^>]*>(.*?)</t[hd]>")

    records: List[Record] = []
    seen: dict = {}
    suspicious_rows: List[str] = []

    for table_match in table_pattern.finditer(html_body):
        table_html = table_match.group(0)
        table_inner = table_match.group(1)
        rows = row_pattern.findall(table_inner)
        if not rows:
            continue

        # Header is normally the first row
        headers_raw = cell_pattern.findall(rows[0])
        if not headers_raw:
            continue
        headers = parse_header_candidates(clean_text("|".join(headers_raw)))
        headers = [h for h in headers if h]
        if not is_municipality_table(headers):
            continue

        target_idx = find_target_column(headers)
        if target_idx is None:
            continue

        try:
            kommune_idx = headers.index("kommune")
        except ValueError:
            # sometimes encoding may alter whitespace/case; do fuzzy scan.
            kommune_idx = -1
            for i, h in enumerate(headers):
                if "kommune" in h:
                    kommune_idx = i
                    break
            if kommune_idx == -1:
                continue

        county_name = extract_county_name_before_table(html_body, table_match.start())
        for row in rows[1:]:
            cells = cell_pattern.findall(row)
            if len(cells) <= max(target_idx, kommune_idx):
                continue

            kommune_name = clean_text(cells[kommune_idx])
            if not kommune_name or kommune_name == "-":
                continue
            kommune_name = sanitize_municipality_name(kommune_name)
            if not kommune_name:
                continue
            if require_reference_match and not is_reference_municipality(
                kommune_name,
                reference_names=reference_municipality_names,
            ):
                county_label = county_name or "Ukjent fylke"
                suspicious_rows.append(f"{county_label} | {kommune_name}")

            value = parse_int_cell(cells[target_idx])
            if value is None:
                continue
            if not include_zero and value == 0:
                continue

            records.append(
                Record(
                    county=county_name or "Ukjent fylke",
                    municipality=kommune_name,
                    accepted_to_settle=value,
                )
            )
            seen_key = (county_name or "Ukjent fylke", kommune_name)
            previous = seen.get(seen_key)
            if previous is None or value > previous:
                seen[seen_key] = value

    # Deduplicate municipality entries if the same table appears multiple times.
    deduped: List[Record] = [
        Record(county=k[0], municipality=k[1], accepted_to_settle=v)
        for k, v in seen.items()
    ]

    if suspicious_rows:
        print(
            f"WARNING: {len(suspicious_rows)} rows look unusual compared to the reference municipality set.",
            file=sys.stderr,
        )
        for item in suspicious_rows[:12]:
            print(f"  - {item}", file=sys.stderr)
        if len(suspicious_rows) > 12:
            print(
                f"  ... and {len(suspicious_rows) - 12} more",
                file=sys.stderr,
            )

    return deduped


def build_probabilities(records: List[Record], decimals: int = 2) -> List[tuple]:
    if not records:
        return []

    total = sum(r.accepted_to_settle for r in records)
    if total <= 0:
        return []

    records_sorted = sorted(records, key=lambda r: r.accepted_to_settle, reverse=True)
    return [
        (
            f"{r.county} | {r.municipality}",
            r.accepted_to_settle,
            round((r.accepted_to_settle / total) * 100, decimals),
        )
        for r in records_sorted
    ]


def write_report(path: str, lines: List[tuple], total: int, decimals: int) -> None:
    with open(path, "w", encoding="utf-8") as file:
        file.write("Kommune - sannsynlighet for bosetting (basert pa antall vedtatte flyktninger)\n")
        file.write(f"Total (sum av vedtatte tall): {total}\n")
        file.write(f"Presisjon: {decimals} desimaler\n")
        file.write("-" * 80 + "\n")
        for name, value, percent in lines:
            file.write(
                f"{name}: {percent:.{decimals}f}%  "
                f"(vedtatt å bosette: {value})\n"
            )


def parse_municipality_query(label: str) -> tuple[str, str]:
    if " | " in label:
        county, municipality = label.split(" | ", 1)
    else:
        county, municipality = "", label
    return county.strip(), municipality.strip()


def lookup_municipality_coords(
    municipality: str,
    municipality_coords: Optional[dict[str, tuple[float, float]]] = None,
) -> Optional[tuple[float, float]]:
    """Look up real coordinates from the selected dictionary."""
    source = municipality_coords if municipality_coords is not None else MUNICIPALITY_COORDS
    coords = source.get(municipality)
    if coords is not None:
        return coords
    municipality_lower = municipality.lower()
    # Pass 1: case-insensitive exact match, then qualifier-stripped exact match
    # (e.g. "Os" should match "Os (Hedm.)" before "Oslo")
    for key, val in source.items():
        if key.lower() == municipality_lower:
            return val
        key_stripped = re.sub(r"\s*\([^)]+\)\s*$", "", key).strip().lower()
        if key_stripped == municipality_lower:
            return val
    # Pass 2: prefix match as a last resort (only for partial names like "Oslo kommune")
    for key, val in source.items():
        if key.startswith(municipality) or municipality.startswith(key):
            return val
    return None


def lookup_cached_point(
    cache: dict | None,
    key: str,
    strict_no_cache: bool = False,
) -> Optional[tuple[float, float]]:
    if not isinstance(cache, dict):
        return None
    normalized_key = (key or "").strip().lower()
    if not normalized_key:
        return None

    cached = cache.get(normalized_key)
    if not strict_no_cache and cached == "not_found":
        return None
    if isinstance(cached, list) and len(cached) == 2:
        try:
            return float(cached[0]), float(cached[1])
        except (TypeError, ValueError):
            pass

    for cache_key, value in cache.items():
        if not isinstance(cache_key, str):
            continue
        cache_key_norm = cache_key.lower().strip()
        if cache_key_norm == normalized_key:
            if not strict_no_cache and value == "not_found":
                return None
        if (
            cache_key_norm == normalized_key
            or cache_key_norm.startswith(normalized_key)
            or normalized_key.startswith(cache_key_norm)
        ):
            if not strict_no_cache and value == "not_found":
                continue
            if isinstance(value, list) and len(value) == 2:
                try:
                    return float(value[0]), float(value[1])
                except (TypeError, ValueError):
                    continue
    return None


def is_in_norway_bounds(lat: float, lon: float, bounds: tuple[float, float, float, float] = NORWAY_BOUNDS) -> bool:
    min_lat, min_lon, max_lat, max_lon = bounds
    return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon


def lookup_strict_municipality_coords(
    strict_municipality_coords: Optional[dict[str, tuple[float, float]]],
    municipality: str,
    county: str | None = None,
) -> Optional[tuple[float, float]]:
    if not isinstance(strict_municipality_coords, dict):
        return None

    strict_key = strict_municipality_cache_key(municipality, county)
    if not strict_key:
        return None

    point = strict_municipality_coords.get(strict_key)
    if isinstance(point, (list, tuple)) and len(point) == 2:
        try:
            return float(point[0]), float(point[1])
        except (TypeError, ValueError):
            return None

    county_norm = normalize_label(county or "")
    if county_norm:
        return None

    legacy_key = normalize_label(municipality)
    if not legacy_key:
        return None

    legacy_point = strict_municipality_coords.get(legacy_key)
    if isinstance(legacy_point, (list, tuple)) and len(legacy_point) == 2:
        try:
            return float(legacy_point[0]), float(legacy_point[1])
        except (TypeError, ValueError):
            return None

    return None


def build_municipality_query_variants(
    municipality: str,
    county: str,
) -> List[str]:
    municipality_clean = (municipality or "").strip()
    county_clean = (county or "").strip()
    if not municipality_clean:
        return []

    queries: List[str] = [municipality_clean]
    if "kommune" not in municipality_clean.lower():
        queries.append(f"{municipality_clean} kommune")
        queries.append(f"{municipality_clean} kommunesenter")
        queries.append(f"{municipality_clean} municipal center")

    normalized_county = normalize_label(county_clean)
    if county_clean and normalized_county != "ukjent fylke":
        queries.append(f"{municipality_clean}, {county_clean}")
        queries.append(f"{municipality_clean} kommune, {county_clean}")
        queries.append(f"{municipality_clean} kommunesenter, {county_clean}")

    queries.extend(
        [
            f"{municipality_clean}, Norway",
            f"{municipality_clean} kommune, Norway",
        ]
    )

    return list(dict.fromkeys(q for q in queries if q))


def normalize_label(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def geocode_cache_key(query: str, provider: str, strict_no_cache: bool = False) -> str:
    key = query.lower().strip()
    if strict_no_cache:
        return f"__strict_no_cache__:{provider}:{key}"
    return f"{provider}:{key}"


def has_boundary_match(value: str, needle: str) -> bool:
    haystack = normalize_label(value)
    needle = normalize_label(needle)
    if not haystack or not needle:
        return False
    if haystack == needle:
        return True
    pattern = rf"(?:^|[\W_]){re.escape(needle)}(?=[\W_]|$)"
    return re.search(pattern, haystack) is not None


def county_aliases(county: str) -> set[str]:
    normalized = normalize_label(county)
    if not normalized:
        return set()

    aliases = {normalized}
    merged = {
        "akershus": {"akershus", "viken"},
        "buskerud": {"buskerud", "viken"},
        "østfold": {"østfold", "viken"},
        "vestfold": {"vestfold", "telemark", "vestfold og telemark"},
        "telemark": {"telemark", "vestfold", "vestfold og telemark"},
        "aust-agder": {"aust-agder", "agder"},
        "vest-agder": {"vest-agder", "agder"},
        "troms": {"troms", "troms og finnmark"},
        "finnmark": {"finnmark", "troms og finnmark"},
        "sør-trøndelag": {"sør-trøndelag", "trøndelag"},
        "nord-trøndelag": {"nord-trøndelag", "trøndelag"},
    }
    if normalized in merged:
        aliases.update(merged[normalized])

    aliases.update(
        {
            normalized.replace("fylke", "").strip(),
            normalized.replace("-", " "),
            normalized.replace(" ", "-"),
        }
    )
    aliases.discard("")
    return aliases


def county_matches(expected_county: str, candidate_county: str) -> bool:
    expected = {normalize_label(v) for v in county_aliases(expected_county)}
    candidate_variants = {normalize_label(candidate_county)}
    candidate_variants.add(candidate_variants.copy().pop().replace("-", " "))
    candidate_variants.add(candidate_variants.copy().pop().replace(" ", "-"))
    candidate_variants.discard("")
    return any(
        e == c or e in c or c in e
        for e in expected
        for c in candidate_variants
    )


def reverse_geocode_point(
    lat: float,
    lon: float,
    reverse_zoom: int = DEFAULT_REVERSE_ZOOM,
    delay_seconds: float = 1.0,
    max_retries: int = 3,
) -> Optional[dict]:
    zoom = max(1, min(18, int(reverse_zoom)))
    params = urllib.parse.urlencode(
        {
            "format": "jsonv2",
            "lat": f"{lat:.6f}",
            "lon": f"{lon:.6f}",
            "zoom": zoom,
            "addressdetails": 1,
            "namedetails": 1,
        }
    )
    request_url = f"{NOMINATIM_REVERSE_URL}?{params}"

    for retry in range(max_retries + 1):
        wait = delay_seconds * (2 ** max(retry, 1))
        if wait > 0:
            time.sleep(wait)
        request = urllib.request.Request(
            request_url,
            headers={"User-Agent": PROVIDER_USER_AGENT},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = decode_web_content(response.read(), response.headers.get_content_charset())
                result = json.loads(payload)
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and retry < max_retries:
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                wait = delay_seconds * (2 ** (retry + 1))
                if retry_after:
                    try:
                        wait = max(wait, int(retry_after))
                    except ValueError:
                        pass
                time.sleep(wait)
                continue
            return None
        except Exception:
            return None

        if isinstance(result, dict):
            return result
    return None


def is_water_reverse_point(reverse_data: Optional[dict]) -> bool:
    if not isinstance(reverse_data, dict):
        return False
    cls = str(reverse_data.get("class", "")).lower()
    typ = str(reverse_data.get("type", "")).lower()
    if cls in {"natural", "waterway"}:
        return typ in WATER_REVERSE_TYPES or "water" in typ
    if cls == "landuse":
        return typ in {"reservoir", "glacier", "bare_rock"}
    return typ in WATER_REVERSE_TYPES


def reverse_matches_municipality(
    reverse_data: Optional[dict],
    municipality: str,
    strict: bool = False,
) -> bool:
    if not isinstance(reverse_data, dict):
        return True
    municipality_norm = normalize_label(municipality)
    if not municipality_norm:
        return True
    address = reverse_data.get("address") or {}
    if not isinstance(address, dict):
        return True

    candidates = (
        address.get("municipality"),
        address.get("city"),
        address.get("town"),
        address.get("village"),
        address.get("hamlet"),
        address.get("suburb"),
        address.get("borough"),
    )
    has_any_candidate = False
    for candidate in candidates:
        candidate_norm = normalize_label(candidate)
        if not candidate_norm:
            continue
        has_any_candidate = True
        if has_boundary_match(candidate_norm, municipality_norm):
            return True
    if not has_any_candidate:
        return not strict
    return False


def is_municipality_forward_candidate(
    result: dict,
    municipality: str,
    county: str | None = None,
    strict: bool = False,
) -> bool:
    if not isinstance(result, dict):
        return False

    cls = str(result.get("class", "")).lower()
    typ = str(result.get("type", "")).lower()
    if cls == "boundary":
        if typ and typ not in {"administrative", "municipality"}:
            return False
    elif cls == "place":
        if typ and typ not in {"municipality", "city", "town", "village", "hamlet", "suburb", "borough"}:
            return False
    elif cls:
        return False

    municipality_norm = normalize_label(municipality)
    if not municipality_norm:
        return not strict

    address = result.get("address")
    if not isinstance(address, dict):
        address = {}
    if isinstance(address, dict):
        candidates = (
            address.get("municipality"),
            address.get("city"),
            address.get("town"),
            address.get("village"),
            address.get("hamlet"),
            address.get("suburb"),
            address.get("borough"),
        )
        for candidate in candidates:
            candidate_norm = normalize_label(candidate)
            if not candidate_norm:
                continue
            if has_boundary_match(candidate_norm, municipality_norm):
                return True

    result_name = normalize_label(str(result.get("name", "")))
    if "," in result_name:
        result_name = normalize_label(result_name.split(",")[0])
    if has_boundary_match(result_name, municipality_norm):
        return True
    display_name = normalize_label(str(result.get("display_name", "")))
    if "," in display_name:
        display_name = normalize_label(display_name.split(",")[0])
    if has_boundary_match(display_name, municipality_norm):
        return True

    if not county:
        return not strict

    county_norm = normalize_label(county)
    if not county_norm:
        return not strict
    county_candidates = (
        address.get("state"),
        address.get("state_district"),
        address.get("region"),
        address.get("county"),
        address.get("province"),
    )
    has_county_info = False
    for candidate in county_candidates:
        candidate_norm = normalize_label(candidate)
        if not candidate_norm:
            continue
        has_county_info = True
        if county_matches(county_norm, candidate_norm):
            return True
    if has_county_info:
        return False
    return True


def is_expected_municipality_coordinate(
    lat: float,
    lon: float,
    municipality: str,
    county: str,
    geocode_delay: float,
    reverse_zoom: int = DEFAULT_REVERSE_ZOOM,
    strict: bool = False,
) -> bool:
    if not is_in_norway_bounds(lat, lon):
        return False
    reverse = reverse_geocode_point(lat, lon, reverse_zoom=reverse_zoom, delay_seconds=geocode_delay)
    if reverse is None:
        return False
    country = str(reverse.get("address", {}).get("country", "")).lower()
    country_code = str(reverse.get("address", {}).get("country_code", "")).lower()
    if country_code:
        if country_code != "no":
            return False
    elif country and "norway" not in country and "norge" not in country:
        return False
    if is_water_reverse_point(reverse):
        return False
    if not reverse_matches_municipality(reverse, municipality, strict=strict):
        return False

    normalized_county = normalize_label(county)
    if not normalized_county or normalized_county == "ukjent fylke":
        return True

    address = reverse.get("address") or {}
    if not isinstance(address, dict):
        return not strict

    county_candidates = (
        address.get("county"),
        address.get("state"),
        address.get("state_district"),
        address.get("region"),
        address.get("province"),
    )
    has_county_info = False
    for candidate in county_candidates:
        candidate_norm = normalize_label(candidate)
        if not candidate_norm:
            continue
        has_county_info = True
        if county_matches(normalized_county, candidate_norm):
            return True

    if has_county_info:
        return False
    return True


def resolve_municipality_coordinate(
    municipality: str,
    county: str,
    geocode_cache: Optional[dict],
    geocoder_provider: str,
    geocode_delay: float = 1.0,
    validate_land: bool = False,
    strict_land: bool = False,
    strict_no_cache: bool = False,
    skip_geocode: bool = False,
    strict_municipality_coords: Optional[dict[str, tuple[float, float]]] = None,
    reverse_zoom: int = DEFAULT_REVERSE_ZOOM,
) -> Optional[tuple[float, float]]:
    def _accept_point(point: tuple[float, float]) -> Optional[tuple[float, float]]:
        if not is_in_norway_bounds(point[0], point[1]):
            return None
        if not validate_land:
            return point
        if is_expected_municipality_coordinate(
            point[0],
            point[1],
            municipality,
            county,
            geocode_delay,
            reverse_zoom=reverse_zoom,
            strict=strict_land,
        ):
            return point
        if strict_land and is_expected_municipality_coordinate(
            point[0],
            point[1],
            municipality,
            county,
            geocode_delay,
            reverse_zoom=reverse_zoom,
            strict=False,
        ):
            return point
        return None

    # 1) Prefer hardcoded kommune centers
    location = lookup_municipality_coords(municipality)
    if location is not None:
        accepted = _accept_point(location)
        if accepted is not None:
            return accepted

    # 2) Prefer strict vetted center dictionary when strict mode is enabled.
    if strict_land and strict_municipality_coords is not None:
        location = lookup_strict_municipality_coords(
            strict_municipality_coords,
            municipality,
            county,
        )
        if location is not None:
            accepted = _accept_point(location)
            if accepted is not None:
                return accepted

    if location is not None:
        accepted = _accept_point(location)
        if accepted is not None:
            return accepted

    if geocode_cache is None:
        return None

    query_variants = build_municipality_query_variants(municipality, county)
    if not query_variants:
        return None

    if skip_geocode:
        for query in query_variants:
            query = query.strip()
            if not query:
                continue
            cached_point = lookup_cached_point(
                geocode_cache,
                geocode_cache_key(query, provider=geocoder_provider, strict_no_cache=strict_no_cache),
                strict_no_cache=strict_no_cache,
            )
            if cached_point is None:
                continue
            accepted = _accept_point(cached_point)
            if accepted is not None:
                return accepted
        return None

    if geocoder_provider == "open-meteo":
        provider_chain = ("nominatim", "open-meteo") if (strict_land or validate_land) else ("open-meteo", "nominatim")
    else:
        provider_chain = (geocoder_provider,)

    for query in query_variants:
        for provider in provider_chain:
            candidate = geocode_with_provider(
                query,
                geocode_cache,
                delay_seconds=geocode_delay,
                municipality=municipality,
                county=county,
                force_refresh=validate_land or strict_land or strict_no_cache,
                strict=strict_land,
                strict_no_cache=strict_no_cache,
                provider=provider,
            )
            if candidate is None:
                continue
            accepted = _accept_point(candidate)
            if accepted is not None:
                return accepted
    return None


def load_geocode_cache(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as cache_file:
            data = json.load(cache_file)
            if isinstance(data, dict):
                return data
    except FileNotFoundError:
        pass
    except Exception as exc:
        print(f"WARNING: cannot read geocode cache {path}: {exc}", file=sys.stderr)
    return {}


def save_geocode_cache(path: str, cache: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as cache_file:
            json.dump(cache, cache_file, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"WARNING: cannot write geocode cache {path}: {exc}", file=sys.stderr)


def load_strict_municipality_coords(path: str) -> dict[str, tuple[float, float]]:
    try:
        with open(path, "r", encoding="utf-8") as cache_file:
            data = json.load(cache_file)
    except FileNotFoundError:
        return {}
    except Exception as exc:
        print(f"WARNING: cannot read strict municipality cache {path}: {exc}", file=sys.stderr)
        return {}

    if not isinstance(data, dict):
        return {}

    strict_coords: dict[str, tuple[float, float]] = {}
    min_lat, min_lon, max_lat, max_lon = NORWAY_BOUNDS
    for municipality, point in data.items():
        if not isinstance(municipality, str) or not municipality.strip():
            continue
        if not isinstance(point, (list, tuple)) or len(point) != 2:
            continue
        try:
            lat = float(point[0])
            lon = float(point[1])
        except (TypeError, ValueError):
            continue
        if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
            continue
        county_part, municipality_part = parse_strict_cache_key(municipality)
        if not municipality_part:
            continue

        strict_key = strict_municipality_cache_key(municipality_part, county_part)
        if strict_key:
            strict_coords[strict_key] = (lat, lon)

    return strict_coords


def save_strict_municipality_coords(
    path: str,
    strict_coords: dict[str, tuple[float, float]],
) -> None:
    serializable: dict[str, list[float]] = {}
    for municipality, point in strict_coords.items():
        if not isinstance(point, (list, tuple)) or len(point) != 2:
            continue
        try:
            serializable[municipality] = [float(point[0]), float(point[1])]
        except (TypeError, ValueError):
            continue
    if not serializable:
        serializable = {}
    try:
        with open(path, "w", encoding="utf-8") as cache_file:
            json.dump(serializable, cache_file, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"WARNING: cannot write strict municipality cache {path}: {exc}", file=sys.stderr)


def is_open_meteo_forward_candidate(
    result: dict,
    municipality: str,
    county: str | None = None,
    strict: bool = False,
) -> bool:
    if not isinstance(result, dict):
        return False

    municipality_norm = normalize_label(municipality)
    if not municipality_norm:
        return not strict

    country_code = str(result.get("country_code", "")).lower()
    if country_code and country_code not in {"no", "norway", "norge"}:
        return False

    candidates = (
        result.get("name"),
        result.get("admin1"),
        result.get("admin2"),
        result.get("admin3"),
        result.get("admin4"),
        result.get("county"),
        result.get("municipality"),
        result.get("district"),
        result.get("village"),
    )
    for candidate in candidates:
        candidate_norm = normalize_label(candidate)
        if not candidate_norm:
            continue
        if has_boundary_match(candidate_norm, municipality_norm):
            return True

    if not county:
        return not strict

    county_norm = normalize_label(county)
    if not county_norm:
        return not strict
    county_candidates = (
        result.get("admin1"),
        result.get("admin2"),
        result.get("admin3"),
        result.get("region"),
        result.get("state"),
        result.get("county"),
    )
    has_county_info = False
    for candidate in county_candidates:
        candidate_norm = normalize_label(candidate)
        if not candidate_norm:
            continue
        has_county_info = True
        if county_matches(county_norm, candidate_norm):
            return True
    if has_county_info:
        return False
    return True


def geocode_with_provider(
    query: str,
    cache: dict,
    delay_seconds: float = 1.0,
    max_retries: int = 3,
    municipality: str | None = None,
    county: str | None = None,
    force_refresh: bool = False,
    strict: bool = False,
    strict_no_cache: bool = False,
    provider: str = "open-meteo",
) -> Optional[tuple[float, float]]:
    provider = (provider or "open-meteo").lower()
    if provider not in {"nominatim", "open-meteo"}:
        provider = "open-meteo"

    key = geocode_cache_key(query, provider=provider, strict_no_cache=strict_no_cache)
    if not force_refresh:
        cached_point = lookup_cached_point(cache, key, strict_no_cache=strict_no_cache)
        if cached_point is not None:
            return cached_point

    if provider == "open-meteo":
        params = urllib.parse.urlencode(
            {
                "name": query,
                "count": 8,
                "language": "en",
            }
        )
        request_url = f"{OPEN_METEO_GEOCODE_URL}?{params}"
    else:
        params = urllib.parse.urlencode(
            {
                "q": query,
                "format": "json",
                "limit": 8,
                "addressdetails": 1,
                "countrycodes": "no",
                "viewbox": NOMINATIM_VIEWBOX,
                "bounded": 1,
            }
        )
        request_url = f"{NOMINATIM_SEARCH_URL}?{params}"

    for retry in range(max_retries + 1):
        wait = delay_seconds * (2 ** max(retry, 1))
        if wait > 0:
            time.sleep(wait)
        request = urllib.request.Request(
            request_url,
            headers={"User-Agent": PROVIDER_USER_AGENT},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
                payload = decode_web_content(raw, response.headers.get_content_charset())
                parsed = json.loads(payload)
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and retry < max_retries:
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                wait = delay_seconds * (2 ** (retry + 1))
                if retry_after:
                    try:
                        wait = max(wait, int(retry_after))
                    except ValueError:
                        pass
                time.sleep(wait)
                continue
            print(f"WARNING: geocode failed for '{query}': HTTP {exc.code}", file=sys.stderr)
            if exc.code != 429:
                cache[key] = "not_found"
            return None
        except Exception as exc:
            print(f"WARNING: geocode failed for '{query}': {exc}", file=sys.stderr)
            if not force_refresh:
                cache[key] = "not_found"
            return None

        break

    if provider == "open-meteo":
        items = parsed.get("results") if isinstance(parsed, dict) else None
    else:
        items = parsed

    if not isinstance(items, list) or not items:
        if not strict_no_cache:
            cache[key] = "not_found"
        return None

    if municipality:
        municipality = municipality.strip()
    for item in items:
        if not isinstance(item, dict):
            continue

        if municipality and provider == "open-meteo":
            if not is_open_meteo_forward_candidate(
                item,
                municipality,
                county=county,
                strict=strict,
            ):
                continue
            country = str(item.get("country_code", "")).strip().lower()
            if country and country not in {"no", "norway", "norge"}:
                continue
        elif municipality and not is_municipality_forward_candidate(
            item,
            municipality,
            county=county,
            strict=strict,
        ):
            continue

        if provider == "open-meteo":
            lat = item.get("latitude")
            lon = item.get("longitude")
        else:
            lat = item.get("lat")
            lon = item.get("lon")
        if lat is None or lon is None:
            continue
        try:
            point = (float(lat), float(lon))
        except (TypeError, ValueError):
            continue
        min_lat, min_lon, max_lat, max_lon = NORWAY_BOUNDS
        if not (min_lat <= point[0] <= max_lat and min_lon <= point[1] <= max_lon):
            continue

        if not strict_no_cache:
            cache[key] = list(point)
        if delay_seconds > 0:
            time.sleep(delay_seconds)
        return point

    if not strict_no_cache:
        cache[key] = "not_found"
    return None


def build_heatmap_html(
    rows: List[tuple],
    output_path: str,
    geocode_cache_path: str,
    geocode_delay: float = 1.0,
    skip_geocode: bool = False,
    show_markers: bool = False,
    validate_land: bool = False,
    strict_land: bool = False,
    strict_no_cache: bool = False,
    geocoder_provider: str = "open-meteo",
    strict_municipality_coords: Optional[dict[str, tuple[float, float]]] = None,
    strict_municipality_verified: Optional[dict[str, tuple[float, float]]] = None,
    include_zero: bool = False,
    reverse_zoom: int = DEFAULT_REVERSE_ZOOM,
    map_min_zoom: int = DEFAULT_MAP_MIN_ZOOM,
    map_max_zoom: int = DEFAULT_MAP_MAX_ZOOM,
    cluster_markers: bool = False,
    marker_cluster_radius: int = DEFAULT_MARKER_CLUSTER_RADIUS,
) -> tuple[int, int]:
    geocode_cache = {} if strict_no_cache else load_geocode_cache(geocode_cache_path)
    points: List[dict] = []
    not_found: List[str] = []
    max_value = 0

    for name, value, percent in rows:
        if not include_zero and value <= 0:
            continue
        county, municipality = parse_municipality_query(name)

        # 1) Built-in dictionary, then optional online lookup.
        location = resolve_municipality_coordinate(
            municipality=municipality,
            county=county,
            geocode_cache=geocode_cache,
            geocoder_provider=geocoder_provider,
            geocode_delay=max(0.0, geocode_delay),
            validate_land=validate_land,
            strict_land=strict_land,
            strict_no_cache=strict_no_cache,
            skip_geocode=skip_geocode,
            strict_municipality_coords=strict_municipality_coords,
            reverse_zoom=reverse_zoom,
        )

        if location is None:
            not_found.append(municipality)
            print(f"WARNING: no coordinates for '{municipality}' ({county})", file=sys.stderr)
            continue

        lat, lon = location
        max_value = max(max_value, value)
        points.append(
            {
                "name": name,
                "lat": lat,
                "lon": lon,
                "value": value,
                "percent": percent,
            }
        )
        if strict_land and strict_municipality_verified is not None:
            strict_key = strict_municipality_cache_key(municipality, county)
            if strict_key:
                strict_municipality_verified[strict_key] = (lat, lon)

    if not strict_no_cache:
        save_geocode_cache(geocode_cache_path, geocode_cache)

    if not points:
        return 0, len(not_found)

    points_json = json.dumps(points, ensure_ascii=False)
    template = f"""<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <title>IMDi bosetting 2026 – heatmap</title>
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\" />
  <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css\" />
  <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css\" />
  <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\"></script>
  <script src=\"https://unpkg.com/leaflet.heat/dist/leaflet-heat.js\"></script>
  <script src=\"https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js\"></script>
  <style>
    html, body {{ margin: 0; padding: 0; width: 100%; height: 100%; }}
    #map {{ width: 100%; height: 100%; }}
  </style>
</head>
<body>
  <div id=\"map\"></div>
  <script>
    const points = {points_json};
    const map = L.map('map', {{ minZoom: {map_min_zoom}, maxZoom: {map_max_zoom} }});
    const baseLayer = L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      maxZoom: {map_max_zoom},
      attribution: '&copy; OpenStreetMap contributors'
    }}).addTo(map);

    const heatData = points.map(p => [p.lat, p.lon, p.value]);
    const maxValue = {max_value};
    const maxPercent = points.reduce((acc, item) => Math.max(acc, item.percent), 0);
    const showMarkers = {str(show_markers).lower()};

    const heatLayer = L.heatLayer(heatData, {{
      radius: 24,
      blur: 16,
      maxZoom: 10,
      max: maxValue,
      minOpacity: 0.35,
      gradient: {{
        0.0: '#0000ff',
        0.25: '#00ffff',
        0.5: '#00ff00',
        0.75: '#ffff00',
        1.0: '#ff0000'
      }}
    }});

    heatLayer.addTo(map);

    const layers = {{
      "Теплова карта (інтенсивність)": heatLayer
    }};

    if (showMarkers) {{
      const useMarkerCluster = {str(cluster_markers).lower()};
      const markerLayer = useMarkerCluster
        ? L.markerClusterGroup({{
            maxClusterRadius: {marker_cluster_radius},
            disableClusteringAtZoom: 11
          }})
        : L.layerGroup();
      for (const p of points) {{
        const intensity = maxPercent > 0 ? (p.percent / maxPercent) : 0;
        const size = 3 + Math.max(2, intensity) * 10;
        const hue = 240 - 240 * intensity;
        const color = `hsl(${{hue}}, 90%, 42%)`;
        const marker = L.circleMarker([p.lat, p.lon], {{
          radius: size,
          color,
          fillColor: color,
          fillOpacity: 0.38,
          weight: 1,
          opacity: 0.8
        }});
        marker.bindTooltip(
          `<b>${{p.name}}</b><br/>Ймовірність: ${{p.percent.toFixed(2)}}%<br/>` +
          `Підстав: ${{p.value}}`,
          {{ sticky: true }}
        );
        marker.bindPopup(`<b>${{p.name}}</b><br/>Ймовірність: ${{p.percent.toFixed(2)}}%<br/>Підстав: ${{p.value}}`);
        markerLayer.addLayer(marker);
      }}
      layers["Точки за ймовірністю"] = markerLayer;
      markerLayer.addTo(map);
    }}

    const bounds = L.latLngBounds(points.map(p => [p.lat, p.lon]));
    map.fitBounds(bounds, {{ padding: [24, 24] }});
    L.control.layers({{}}, layers, {{ collapsed: true }}).addTo(map);

    const legend = L.control({{ position: 'bottomright' }});
    legend.onAdd = function () {{
      const div = L.DomUtil.create('div', 'info legend');
      div.style.background = 'white';
      div.style.padding = '8px';
      div.style.borderRadius = '6px';
      div.style.lineHeight = '1.25';
      div.style.fontSize = '12px';
      div.style.boxShadow = '0 0 8px rgba(0,0,0,0.2)';
      div.innerHTML = `
        <div style="font-weight:700;margin-bottom:4px;">Шкала ймовірності</div>
        <div style="display:flex;align-items:center;gap:4px;margin-top:4px;">
          <span style="display:inline-block;width:26px;height:12px;background:#0000ff;"></span> 0%
        </div>
        <div style="display:flex;align-items:center;gap:4px;">
          <span style="display:inline-block;width:26px;height:12px;background:#00ffff;"></span> 0.5%
        </div>
        <div style="display:flex;align-items:center;gap:4px;">
          <span style="display:inline-block;width:26px;height:12px;background:#00ff00;"></span> 1%
        </div>
        <div style="display:flex;align-items:center;gap:4px;">
          <span style="display:inline-block;width:26px;height:12px;background:#ffff00;"></span> 3%
        </div>
        <div style="display:flex;align-items:center;gap:4px;">
          <span style="display:inline-block;width:26px;height:12px;background:#ff0000;"></span> 6%+ 
        </div>
      `;
      return div;
    }};
    legend.addTo(map);
  </script>
</body>
</html>
"""
    with open(output_path, "w", encoding="utf-8") as file:
        file.write(template)

    return len(points), len(not_found)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate settlement probability per municipality from IMDi statistics."
    )
    parser.add_argument(
        "--url",
        default=URL_DEFAULT,
        help="Source page URL",
    )
    parser.add_argument(
        "--output",
        default="kommuner_sannsynlighet_bosetting_2026.txt",
        help="Output text file path",
    )
    parser.add_argument(
        "--decimals",
        type=int,
        default=2,
        choices=range(0, 10),
        metavar="[0-9]",
        help="Number of decimal digits in percent",
    )
    parser.add_argument(
        "--skip-zero",
        action="store_true",
        help="Do not include municipalities with accepted_to_settle == 0",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=0,
        help="If > 0, keep only top N municipalities",
    )
    parser.add_argument(
        "--map-output",
        help="Output HTML path for heatmap (Leaflet), e.g. kommuner_bosetting_heatmap_2026.html",
    )
    parser.add_argument(
        "--map-top",
        type=int,
        default=0,
        help="If > 0, build heatmap from top N municipalities",
    )
    parser.add_argument(
        "--geocode-cache",
        default="kommuner_geocode_cache_2026.json",
        help="JSON cache path for municipality geocoding",
    )
    parser.add_argument(
        "--geocode-delay",
        type=float,
        default=1.1,
        help="Delay in seconds between geocode requests",
    )
    parser.add_argument(
        "--geocoder-provider",
        default="open-meteo",
        choices=["nominatim", "open-meteo"],
        help="Forward geocoding provider for missed municipalities (default: open-meteo).",
    )
    parser.add_argument(
        "--skip-geocode",
        action="store_true",
        help=(
            "Do not call online geocoding API. "
            "Uses only built-in municipality coordinates and cached geocode values."
        ),
    )
    parser.add_argument(
        "--validate-land",
        action="store_true",
        help=(
            "Validate coordinates via reverse geocoding and retry online geocoding if static coordinates fail county/municipality checks."
            " If no validated point is found, municipality is skipped."
        ),
    )
    parser.add_argument(
        "--strict-land",
        action="store_true",
        help=(
            "Apply the strictest land validation policy: candidates must match both municipality and county. "
            "If strict mode is enabled, not_found cache entries are bypassed and weak reverse matches are rejected."
        ),
    )
    parser.add_argument(
        "--strict-municipality-cache",
        default=STRICT_COORDINATES_PATH_DEFAULT,
        help=(
            "Path for strict-mode verified municipality coordinates (overwrites on each strict run). "
            "Keys are stored as county|municipality where possible to avoid name collisions."
        ),
    )
    parser.add_argument(
        "--strict-no-cache",
        action="store_true",
        help=(
            "Do not read/write geocode cache and validate coordinates only via fresh API lookups. "
            "This mode also enables strict-land validation and starts from empty strict coordinates."
        ),
    )
    parser.add_argument(
        "--require-source-match",
        action="store_true",
        help=(
            "Skip municipality rows from source table that cannot be matched to a known municipality reference "
            "(defensive mode against malformed site data)."
        ),
    )
    parser.add_argument(
        "--show-markers",
        action="store_true",
        help="Display per-commune markers in addition to heat layer",
    )
    parser.add_argument(
        "--include-zero-points",
        action="store_true",
        help=(
            "Keep municipalities with zero accepted_to_settle in heatmap output "
            "(by default only municipalities with value > 0 are rendered)."
        ),
    )
    parser.add_argument(
        "--reverse-zoom",
        type=int,
        default=DEFAULT_REVERSE_ZOOM,
        help="Reverse geocode zoom level used for municipality validation.",
    )
    parser.add_argument(
        "--map-min-zoom",
        type=int,
        default=DEFAULT_MAP_MIN_ZOOM,
        help="Minimum zoom for generated map.",
    )
    parser.add_argument(
        "--map-max-zoom",
        type=int,
        default=DEFAULT_MAP_MAX_ZOOM,
        help="Maximum zoom for base map tiles.",
    )
    parser.add_argument(
        "--cluster-markers",
        action="store_true",
        help="Render municipality markers in clustered mode when markers are enabled.",
    )
    parser.add_argument(
        "--marker-cluster-radius",
        type=int,
        default=DEFAULT_MARKER_CLUSTER_RADIUS,
        help="Cluster radius in pixels for Leaflet marker clustering.",
    )
    return parser.parse_args()


def fetch_html(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; IMDiSettlementsBot/1.0)"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        raw = response.read()
    return decode_web_content(raw, charset)


def main() -> int:
    args = parse_args()
    if args.strict_land:
        args.validate_land = True
    if args.strict_no_cache:
        args.validate_land = True
        args.strict_land = True
    reference_municipality_names = load_reference_municipality_names(
        args.strict_municipality_cache
    )
    strict_municipality_coords: Optional[dict[str, tuple[float, float]]] = None
    strict_municipality_verified: dict[str, tuple[float, float]] | None = None
    if args.strict_land:
        if args.strict_no_cache:
            strict_municipality_coords = {}
        else:
            strict_municipality_coords = load_strict_municipality_coords(args.strict_municipality_cache)
        strict_municipality_verified = {}
    try:
        html_body = fetch_html(args.url)
    except Exception as exc:
        print(f"ERROR: Failed to fetch URL {args.url}: {exc}", file=sys.stderr)
        return 1

    records = parse_tables(
        html_body,
        include_zero=not args.skip_zero,
        reference_municipality_names=reference_municipality_names,
        require_reference_match=args.require_source_match,
    )
    if not records:
        print("ERROR: No municipality rows were parsed.", file=sys.stderr)
        return 1

    total = sum(r.accepted_to_settle for r in records)
    rows = build_probabilities(records, decimals=args.decimals)
    rows_for_report = rows
    if args.top and args.top > 0:
        rows_for_report = rows[: args.top]

    write_report(args.output, rows_for_report, total=total, decimals=args.decimals)

    if args.map_output:
        map_rows = rows
        if args.map_top and args.map_top > 0:
            map_rows = rows[: args.map_top]
        mapped, missing = build_heatmap_html(
            map_rows,
            output_path=args.map_output,
            geocode_cache_path=args.geocode_cache,
            geocode_delay=max(0.0, args.geocode_delay),
            skip_geocode=args.skip_geocode,
            show_markers=args.show_markers,
            include_zero=args.include_zero_points,
            validate_land=args.validate_land,
            strict_land=args.strict_land,
            strict_no_cache=args.strict_no_cache,
            geocoder_provider=args.geocoder_provider,
            strict_municipality_coords=strict_municipality_coords,
            strict_municipality_verified=strict_municipality_verified,
            reverse_zoom=args.reverse_zoom,
            map_min_zoom=args.map_min_zoom,
            map_max_zoom=args.map_max_zoom,
            cluster_markers=args.cluster_markers,
            marker_cluster_radius=args.marker_cluster_radius,
        )
        print(f"Heatmap: {mapped} points written to {args.map_output}")
        if missing:
            print(f"Heatmap: {missing} municipalities were not geocoded")
        if args.strict_land and strict_municipality_verified is not None:
            save_strict_municipality_coords(
                args.strict_municipality_cache,
                strict_municipality_verified,
            )

    print(f"Done. Output: {args.output}")
    print(f"Parsed municipalities: {len(rows_for_report)}")
    if args.top and args.top > 0:
        print(f"Top limit: {args.top}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
