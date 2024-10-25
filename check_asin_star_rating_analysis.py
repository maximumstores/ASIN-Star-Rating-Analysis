import os
import requests
from bs4 import BeautifulSoup 
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
import time
from gspread_formatting import (
    format_cell_range, 
    format_cell_ranges, 
    CellFormat, 
    Color, 
    TextFormat
)
import random
import json
import pytz
from datetime import datetime, timedelta
import telebot
from babel.numbers import parse_decimal, NumberFormatError
from collections import defaultdict
from gspread.exceptions import APIError
import traceback
from babel.numbers import parse_decimal, NumberFormatError
from babel.core import Locale
import logging

from gspread.exceptions import APIError
# Попытка импорта CellNotFound, если доступен
try:
    from gspread.exceptions import CellNotFound
    HAS_CELLNOTFOUND = True
except ImportError:
    HAS_CELLNOTFOUND = False
from gspread_formatting import CellFormat, Color
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Определение уровней звезд
stars = {
    5: 'five_star',
    4: 'four_star',
    3: 'three_star',
    2: 'two_star',
    1: 'one_star'
}

class RateLimiter:
    def __init__(self, max_requests, per_seconds):
        self.max_requests = max_requests
        self.per_seconds = per_seconds
        self.requests = []
    
    def wait(self):
        now = time.time()
        
        # Удаляем старые запросы
        self.requests = [req for req in self.requests if now - req < self.per_seconds]
        
        if len(self.requests) >= self.max_requests:
            sleep_time = self.requests[0] + self.per_seconds - now
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        self.requests.append(time.time())

# Создаем глобальные экземпляры ограничителей скорости
read_limiter = RateLimiter(max_requests=50, per_seconds=60)  # 50 запросов в минуту для безопасности
write_limiter = RateLimiter(max_requests=50, per_seconds=60)


def get_raw_html(url, auth, timeout=60):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.post(
            'https://realtime.oxylabs.io/v1/queries',
            auth=auth,
            json={'source': 'amazon', 'url': url, 'parse': False},
            headers=headers,
            timeout=timeout
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при отправке запроса: {e}")
        return None

    try:
        response_json = response.json()
        results = response_json.get('results', [])
        if not results:
            logging.warning("Нет результатов в ответе API.")
            return None
        return results[0].get('content', '')
    except ValueError as e:
        logging.error(f"Ошибка при разборе JSON: {e}")
        return None
    



def parse_total_ratings(html_content, star_label, region):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Определяем локаль на основе региона
    region_to_locale = {
        'com': 'en_US',
        'co.uk': 'en_GB',
        'de': 'de_DE',
        'fr': 'fr_FR',
        'it': 'it_IT',
        'es': 'es_ES',
        'ca': 'en_CA',
        # Добавьте другие регионы и локали по необходимости
    }
    
    locale_str = region_to_locale.get(region, 'en_US')
    
    selectors = [
        ('span', {'data-hook': 'total-review-count'}),
        ('div', {'data-hook': 'cr-filter-info-review-rating-count'}),
        ('span', {'class_': 'totalReviewCount'}),
        ('span', {'data-hook': 'rating-count'}),
        ('div', {'class_': 'a-row a-spacing-base a-size-base'})
    ]
    
    total_ratings_str = None
    for tag, attrs in selectors:
        element = soup.find(tag, attrs)
        if element:
            total_ratings_str = element.get_text(strip=True)
            logging.info(f"Найден текст отзывов для {star_label} в регионе {region}: '{total_ratings_str}'")
            break

    if not total_ratings_str:
        filter_section = soup.find('div', {'class': 'a-section reviews-filter-info-section'})
        if filter_section:
            total_ratings_str = filter_section.get_text(strip=True)
            logging.info(f"Найден альтернативный текст отзывов: '{total_ratings_str}'")

    if total_ratings_str:
        patterns = {
            'uk': [
                r'(\d{1,3}(?:,\d{3})*)\s+total ratings,\s+(\d{1,3}(?:,\d{3})*)\s+with reviews',
                r'(\d+)\s+total ratings?,\s+(\d+)\s+with reviews?',
                r'(\d+)\s+total ratings',
            ],
            'de_DE': [
                r'(\d{1,3}(?:\.\d{3})*)\s+Gesamtbewertungen,\s+(\d+)\s+mit Rezensionen',
                r'(\d+)\s+Gesamtbewertungen',
                r'(\d{1,3}(?:[.,]\d{3})*)\s+total ratings,\s+(\d+)\s+(?:mit Rezensionen|with reviews)',
                r'(\d+)\s+total ratings',
            ],
            'fr_FR': [
                r'(\d{1,3}(?:\s?\d{3})*)\s+évaluations au total,\s+(\d+)\s+avec avis',
                r'(\d+)\s+évaluations au total',
            ],
            'it_IT': [
                r'(\d{1,3}(?:\.\d{3})*)\s+valutazioni totali,\s+(\d+)\s+con recensioni',
                r'(\d+)\s+valutazioni totali',
            ],
            'es_ES': [
                r'(\d{1,3}(?:\.\d{3})*)\s*valoraciones totales,\s*(\d+)\s*con reseñas',
                r'(\d+)\s*valoraciones totales',
            ],
            'ca': [  # Добавляем для канадского английского, если необходимо
                r'(\d{1,3}(?:,\d{3})*)\s+total ratings,\s+(\d{1,3}(?:,\d{3})*)\s+with reviews',
                r'(\d+)\s+total ratings?,\s+(\d+)\s+with reviews?',
                r'(\d+)\s+total ratings',
            ],
            'default': [
                r'(\d{1,3}(?:,\d{3})*)\s+total ratings,\s+(\d{1,3}(?:,\d{3})*)\s+with reviews',
                r'(\d+)\s+total ratings?,\s+(\d+)\s+with reviews?',
                r'(\d+)\s+total ratings',
            ]
        }

        locale_patterns = patterns.get(locale_str, patterns['default'])

        for pattern in locale_patterns:
            match = re.search(pattern, total_ratings_str, re.IGNORECASE)
            if match:
                total = match.group(1)
                with_reviews = match.group(2) if len(match.groups()) > 1 else '0'
                
                logging.debug(f"Шаблон: {pattern}")
                logging.debug(f"Группа 1 (Total): {total}")
                logging.debug(f"Группа 2 (Reviews): {with_reviews}")
                
                # Обработка чисел в соответствии с локалью
                total = parse_number(total, locale_str)
                with_reviews = parse_number(with_reviews, locale_str)

                if total is not None and with_reviews is not None:
                    logging.info(f"Извлечены данные: Total Ratings = {total}, Reviews with text = {with_reviews}")
                    return {
                        'Total Ratings': str(int(total)),
                        'Reviews with text': str(int(with_reviews))
                    }
                else:
                    logging.warning(f"Не удалось распарсить числа: Total = {total}, Reviews = {with_reviews}")
                    return {
                        'Total Ratings': 'Нет данных',
                        'Reviews with text': 'Нет данных'
                    }

    logging.warning(f"Не удалось извлечь данные из строки. Текст: {total_ratings_str}")
    return {'Total Ratings': 'Нет данных', 'Reviews with text': 'Нет данных'}

def process_asin_by_country(stars, asin, region, auth):
    if region == 'co.uk':
        base_url = f'https://www.amazon.co.uk/product-reviews/{asin}/ref=acr_dpx_hist_{{star_label}}?ie=UTF8&filterByStar={{star_label}}&reviewerType=all_reviews&pageNumber=1#reviews-filter-bar'
    else:
        base_url = f'https://www.amazon.{region}/product-reviews/{asin}/ref=acr_dpx_hist_{{star_label}}?ie=UTF8&filterByStar={{star_label}}&reviewerType=all_reviews&pageNumber=1#reviews-filter-bar'

    star_data = {}

    for star_number, star_label in stars.items():
        # Формируем URL для каждого уровня звезд
        url = base_url.format(star_label=star_label)

        logging.info(f"\nОбрабатывается {star_number}-звёздочный отзыв для ASIN {asin} в регионе {region}: {url}")
        logging.info(f"Сформированный URL: {url}")  # Дополнительное логирование

        # Получаем HTML содержимое
        html_content = get_raw_html(url, auth)

        if html_content:
            # Парсим данные о количестве отзывов
            data = parse_total_ratings(html_content, star_label, region)
            star_data[star_label] = data
        else:
            star_data[star_label] = {
                'Total Ratings': 'Ошибка получения данных',
                'Reviews with text': 'Ошибка получения данных'
            }

    logging.info(f"\nРезультаты по звёздам для ASIN {asin} в регионе {region}:")
    for star_number, star_label in stars.items():
        data = star_data.get(star_label, {})
        logging.info(f"{star_number} звёзд: Total Ratings: {data.get('Total Ratings')}, Reviews with text: {data.get('Reviews with text')}")
    logging.info("==============================================")

    return star_data


# Ваши остальные функции и код...
#     
# Маппинг доменов Amazon на локали
DOMAIN_LOCALE_MAP = {
    'com': 'en_US',
    'co.uk': 'en_GB',
    'de': 'de_DE',
    'fr': 'fr_FR',
    'it': 'it_IT',
    'es': 'es_ES',
    'ca': 'en_CA',  # или 'fr_CA' в зависимости от потребности
    'co.jp': 'ja_JP',
    'com.au': 'en_AU',
    'nl': 'nl_NL',
    'se': 'sv_SE',
    'pl': 'pl_PL',
    # Добавьте другие домены по необходимости
}

# Маппинг локалей на названия категории "Fashion" на соответствующих языках
# Маппинг локалей на названия категорий
LOCALE_CATEGORY_MAP = {
    'en_US': ['Fashion', 'Clothing', 'Underwear', 'Thermal', 'Thermal Tops', 'Thermal T-Shirt', 'Moda'],
    'de_DE': ['Fashion', 'Clothing', 'Unterwäsche', 'Thermal', 'Thermal Tops', 'Thermal T-Shirt', 'Moda'],
    'fr_FR': ['Fashion', 'Vêtements', 'Sous-vêtements', 'Thermal', 'Thermal Tops', 'Thermal T-Shirt', 'Moda'],
    'it_IT': ['Fashion', 'Moda', 'Abbigliamento', 'Intimo', 'Thermal', 'Thermal Tops', 'Thermal T-Shirt'],
    'es_ES': ['Fashion', 'Moda', 'Ropa', 'Ropa Interior', 'Thermal', 'Thermal Tops', 'Thermal T-Shirt'],
    'en_GB': ['Fashion', 'Clothing', 'Underwear', 'Thermal', 'Thermal Tops', 'Thermal T-Shirt', 'Moda'],
    'en_CA': ['Fashion', 'Clothing', 'Underwear', 'Thermal', 'Thermal Tops', 'Thermal T-Shirt', 'Moda'],
    'ja_JP': ['ファッション', 'アパレル', '下着', 'サーマル', 'サーマルトップス', 'サーマルTシャツ'],
    'en_AU': ['Fashion', 'Clothing', 'Underwear', 'Thermal', 'Thermal Tops', 'Thermal T-Shirt', 'Moda'],
    'nl_NL': ['Fashion', 'Kleding', 'Ondergoed', 'Thermal', 'Thermal Tops', 'Thermal T-Shirt', 'Moda'],
    'sv_SE': ['Fashion', 'Kläder', 'Underkläder', 'Thermal', 'Thermal Tops', 'Thermal T-Shirt', 'Moda'],
    'pl_PL': ['Fashion', 'Moda', 'Odzież', 'Bielizna', 'Thermal', 'Thermal Tops', 'Thermal T-Shirt'],
}
# Карта локали к терминам моды для разных стран
LOCALE_FASHION_MAP = {
    'en_US': ['Fashion'],
    'de_DE': ['Fashion', 'Mode'],
    'fr_FR': ['Fashion', 'Vêtements', 'Sous-vêtements', 'Thermal', 'Thermal Tops', 'Thermal T-Shirt', 'Moda'],
    'it_IT': ['Fashion', 'Moda', 'Abbigliamento', 'Intimo', 'Thermal', 'Thermal Tops', 'Thermal T-Shirt'],
    'es_ES': ['Fashion', 'Moda'],
    'en_GB': ['Fashion', 'Moda'],
    'en_CA': ['Fashion'],
    'ja_JP': ['ファッション'],  # Японский термин для Fashion
    'en_AU': ['Fashion'],
    'nl_NL': ['Fashion'],
    'sv_SE': ['Fashion'],
    'pl_PL': ['Fashion'],
    # Добавьте другие локали и соответствующие названия по необходимости
}
def get_locale_from_url(url):
    """Определяет локаль на основе домена Amazon в URL."""
    DOMAIN_LOCALE_MAP = {
        'com': 'en_US',
        'co.uk': 'en_GB',
        'de': 'de_DE',
        'fr': 'fr_FR',
        'it': 'it_IT',
        'es': 'es_ES',
        'ca': 'en_CA',
        'co.jp': 'ja_JP',
        'com.au': 'en_AU',
        'nl': 'nl_NL',
        'se': 'sv_SE',
        'pl': 'pl_PL',
        # Добавьте другие домены по необходимости
    }
    match = re.search(r'https?://www\.amazon\.([a-z.]+)', url)
    if match:
        domain = match.group(1)
        locale = DOMAIN_LOCALE_MAP.get(domain, 'en_US')
        logging.info(f"Определена локаль '{locale}' для домена 'amazon.{domain}'.")
        return locale
    else:
        logging.warning(f"Не удалось определить домен из URL: {url}. Используем 'en_US' по умолчанию.")
        return 'en_US'


def get_decimal_separator(locale_str):
    """Возвращает символ десятичного разделителя для заданной локали."""
    try:
        locale = Locale.parse(locale_str)
        decimal_separator = locale.number_symbols.get('decimal')
        logging.debug(f"Десятичный разделитель для локали '{locale_str}': '{decimal_separator}'")
        return decimal_separator
    except Exception as e:
        logging.error(f"Не удалось определить десятичный разделитель для локали '{locale_str}': {e}")
        return '.'


def get_geo_location_from_locale(locale_str):
    """Возвращает соответствующий почтовый индекс для geo_location на основе локали."""
    locale_geo_map = {
        'en_US': '10001',    # Нью-Йорк, США
        'en_GB': 'EC1A1BB',  # Лондон, Великобритания (без пробела)
        'de_DE': '10115',    # Берлин, Германия
        'fr_FR': '75001',    # Париж, Франция
        'it_IT': '00118',    # Рим, Италия
        'es_ES': '28001',    # Мадрид, Испания
        'en_CA': 'M5H2N2',   # Торонто, Канада (без пробела)
        'ja_JP': '100-0001', # Токио, Япония
        'en_AU': '2000',     # Сидней, Австралия
        'nl_NL': '1011',     # Амстердам, Нидерланды
        'sv_SE': '11122',    # Стокгольм, Швеция (без пробела)
        'pl_PL': '00-001',   # Варшава, Польша
        # Добавьте другие локали по необходимости
    }
    geo_location = locale_geo_map.get(locale_str)
    if geo_location:
        logging.debug(f"Определен geo_location '{geo_location}' для локали '{locale_str}'.")
    else:
        logging.warning(f"Не удалось определить geo_location для локали '{locale_str}'.")
    return geo_location

# Ваши остальные функции и код

def parse_number(number_str, locale_str):
    """Парсит строковое представление числа в соответствии с локалью."""
    try:
        locale = Locale.parse(locale_str)
        number = parse_decimal(number_str, locale=locale)
        return number
    except NumberFormatError:
        # Попытка удалить разделители тысяч и повторить попытку
        number_str_clean = re.sub(r'[^\d.,\-–−]', '', number_str)
        try:
            # Определение символа разделителя десятичных знаков
            decimal_sep = locale.number_symbols.get('decimal', '.')
            if decimal_sep == ',':
                # Удаляем точки (разделители тысяч) и заменяем запятые на точки (десятичные знаки)
                number_str_clean = number_str_clean.replace('.', '').replace(',', '.')
            else:
                # Удаляем запятые (разделители тысяч)
                number_str_clean = number_str_clean.replace(',', '')
            number = float(number_str_clean)
            return number
        except ValueError:
            logging.error(f"Не удалось распарсить число: '{number_str}' с локалью '{locale_str}'.")
            return None




def retry_with_backoff(func, is_read_operation=True, max_retries=10, initial_delay=5, max_delay=300, factor=2):
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            if is_read_operation:
                read_limiter.wait()
            else:
                write_limiter.wait()
            
            return func()
        except gspread.exceptions.APIError as e:
            if hasattr(e, 'response') and e.response.status_code in [429, 500, 503]:
                delay = min(delay * factor, max_delay)
                jitter = random.uniform(0, 0.1 * delay)
                sleep_time = delay + jitter
                logging.warning(f"API Error (attempt {attempt + 1}/{max_retries}). Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            else:
                logging.error(f"Неизвестная ошибка при вызове API: {e.response.text if hasattr(e, 'response') else str(e)}")
                raise
        except json.JSONDecodeError as e:
            logging.error(f"Ошибка декодирования JSON: {e}")
            raise
        except Exception as e:
            logging.error(f"Неожиданная ошибка: {e}")
            raise
    raise Exception(f"Max retries ({max_retries}) exceeded")

def log_quota_usage(spreadsheet):
    try:
        quotas = spreadsheet.client.get_quota()
        logging.info(f"Текущее использование квот:")
        logging.info(f"Write requests per minute: {quotas['writes']['usage']} из {quotas['writes']['limit']} ({quotas['writes']['percentage']:.2f}%)")
        logging.info(f"Read requests per minute: {quotas['reads']['usage']} из {quotas['reads']['limit']} ({quotas['reads']['percentage']:.2f}%)")
    except Exception as e:
        logging.error(f"Не удалось получить информацию о квотах: {e}")

def log_and_format_cell_range(sheet, cell_range, cell_format):
    try:
        logging.info(f"Попытка форматирования ячейки {cell_range}")
        # Извлекаем цвета и формат текста
        bg_color = cell_format.backgroundColor
        text_format = cell_format.textFormat
        logging.debug(f"Применяемый формат: backgroundColor={bg_color}, textFormat={text_format}")
        format_cell_range(sheet, cell_range, cell_format)  # Используем функцию из gspread_formatting
    except Exception as e:
        logging.error(f"Ошибка при форматировании ячейки {cell_range}: {str(e)}")
        logging.debug(f"Содержимое ячейки: {sheet.acell(cell_range).value}")
        raise

def clean_urls(raw_value):
    """Очищает строку URL-адресов от лишних символов и корректирует URL перед отправкой запроса к Oxylabs."""
    # Убираем квадратные скобки, если они есть
    raw_value = raw_value.strip().strip('[]')
    
    # Разбиваем строку на отдельные URL, используя любые пробельные символы как разделители
    urls = re.split(r'\s+', raw_value)
    
    # Убираем любые лишние кавычки вокруг URL и очищаем URL
    cleaned_urls = [re.sub(r'^["\']|["\']$', '', url) for url in urls]
    
    # Убираем пустые строки
    cleaned_urls = [url for url in cleaned_urls if url]
    
    # Убираем части URL вроде '-/en', которые могут мешать API
    cleaned_urls = [re.sub(r'/-/en', '', url) for url in cleaned_urls]
    
    return cleaned_urls


def read_config(sheet):
    """Считывает конфигурацию из листа Config."""
    try:
        config = {}
        records = sheet.get_all_records()
        for record in records:
            key = record.get('Key')
            value = record.get('Value')
            if key and value:
                if key == 'product_urls':
                    # Чистим и разбиваем строку URL
                    config[key] = clean_urls(value)
                else:
                    config[key] = value
        logging.info(f"Загруженная конфигурация: {config}")
        return config
    except Exception as e:
        logging.error(f"Ошибка при считывании конфигурации: {e}")
        raise
    
def clean_amazon_url(url):
    """Очищает или корректирует URL Amazon перед отправкой запроса к Oxylabs."""
    # Убираем части URL вроде '-/en', которые могут мешать API
    cleaned_url = re.sub(r'/-/en', '', url)
   
    # Добавляем другие проверки или модификации при необходимости
    return cleaned_url



def extract_asin(url):
    """Извлекает ASIN из URL Amazon с поддержкой различных форматов."""
    patterns = [
        r'/dp/([A-Z0-9]{10})',
        r'/gp/product/([A-Z0-9]{10})',
        r'/product/([A-Z0-9]{10})',
        r'/exec/obidos/ASIN/([A-Z0-9]{10})',
        r'/[^/]+/-/[a-z]{2}/dp/([A-Z0-9]{10})',  # Для форматов типа https://www.amazon.fr/-/en/dp/B0CZ796K6W
        r'ASIN=([A-Z0-9]{10})',  # Для URL с параметром ASIN
        r'"([A-Z0-9]{10})"'  # Для извлечения ASIN из HYPERLINK формулы
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            asin = match.group(1)
            logging.debug(f"Извлечен ASIN: {asin} из URL: {url}")
            return asin
    
    logging.warning(f"ASIN не найден в URL: {url}")
    return None




def extract_bsr(product_data, locale):
    sales_rank_data = product_data.get('sales_rank', [])
    if not sales_rank_data:
        logging.warning("Данные sales_rank отсутствуют.")
        return 'Not Found'

    category_terms = LOCALE_CATEGORY_MAP.get(locale, ['Fashion'])
    best_rank = None
    best_rank_value = None

    for entry in sales_rank_data:
        rank = entry.get('rank')
        ladder = entry.get('ladder', [])
        # Извлекаем только названия категорий, игнорируя URL
        categories = [cat.get('name', '').strip() for cat in ladder]

        if rank and categories:
            if any(
                any(category_term.lower() in category.lower() for category_term in category_terms)
                for category in categories
            ):
                try:
                    bsr_value = str(rank).replace(',', '').split()[0]
                    logging.debug(f"Найден BSR {bsr_value} в категории '{' > '.join(categories)}'")
                    return bsr_value
                except AttributeError as e:
                    logging.error(f"Ошибка при обработке BSR: {e}")
            else:
                try:
                    rank_value = int(str(rank).replace(',', ''))
                    if best_rank_value is None or rank_value < best_rank_value:
                        best_rank_value = rank_value
                        best_rank = entry
                except (ValueError, AttributeError) as e:
                    logging.error(f"Некорректный формат ранга: {rank} - {e}")

    if best_rank:
        try:
            bsr_value = str(best_rank.get('rank')).replace(',', '').split()[0]
            return bsr_value
        except AttributeError as e:
            logging.error(f"Ошибка при обработке лучшего BSR: {e}")
            return 'Not Found'
    else:
        logging.warning("Ранг не найден в данных sales_rank.")
        return 'Not Found'



def scrape_amazon_product(url, oxylabs_username, oxylabs_password, tz):
    asin = extract_asin(url)
    if not asin:
        logging.error(f"Не удалось извлечь ASIN из URL: {url}")
        return None

    payload = {
        'source': 'amazon',
        'url': url,
        'parse': True
    }
    locale = get_locale_from_url(url)
    
    # Определяем регион на основе локали
    locale_to_region = {
        'en_US': 'com',
        'en_GB': 'co.uk',
        'de_DE': 'de',
        'fr_FR': 'fr',
        'it_IT': 'it',
        'es_ES': 'es',
        'en_CA': 'ca',
        'ja_JP': 'co.jp',
        'en_AU': 'com.au',
        'nl_NL': 'nl',
        'sv_SE': 'se',
        'pl_PL': 'pl',
        # Добавьте другие локали по необходимости
    }
    
    region = locale_to_region.get(locale, 'com')  # По умолчанию 'com'
    logging.info(f"Определён регион '{region}' для локали '{locale}'.")

    try:
        logging.debug(f"Отправка запроса к Oxylabs для URL: {url} с локалью {locale}")
        response = requests.post(
            'https://realtime.oxylabs.io/v1/queries',
            auth=(oxylabs_username, oxylabs_password),
            json=payload,
            timeout=60
        )
        response.raise_for_status()
        response_json = response.json()
        results = response_json.get('results', [])
        if not results:
            logging.warning(f"No results in response for {url}")
            return None
        content = results[0].get('content', {})
        logging.debug(f"Полученный контент: {content}")

        # Извлечение основных данных из API
        title = content.get('title', 'Not Found')
        price = content.get('price', 'Not Found')
        rating = content.get('rating', 'Not Found')
        reviews = content.get('reviews_count', 'Not Found')
        bsr = extract_bsr(content, locale)

        # Сбор данных по звёздам
        star_data = process_asin_by_country(stars, asin, region, (oxylabs_username, oxylabs_password))
        logging.debug(f"Собранные данные по звёздам: {star_data}")

        scrape_date = datetime.now(tz).strftime("%d.%m.%Y")
        return {
            "Title": title,
            "ASIN": asin,
            "Price": price,
            "Rating": rating,
            "Number of Reviews": reviews,
            "Product URL": url,
            "Scrape Date": scrape_date,
            "BSR": bsr,
            **star_data  # Объединение данных по звёздам
        }
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error for {url}: {e}")
        return None




def batch_format_cells(sheet, formatting_updates):
    logging.info(f"Entering batch_format_cells with {len(formatting_updates)} updates")
    if not formatting_updates:
        logging.warning("No formatting updates to apply.")
        return

    valid_updates = []
    for cell_range, rating in formatting_updates:
        logging.debug(f"Processing update: cell_range={cell_range}, rating={rating}")
        # Если диапазон содержит имя листа, отделяем его
        if '!' in cell_range:
            _, cell_range = cell_range.split('!', 1)
        # Проверяем корректность диапазона
        if not re.match(r'^[A-Za-z]+\d+(:[A-Za-z]+\d+)?$', cell_range):
            logging.warning(f"Invalid cell range format: {cell_range}. Skipping.")
            continue
        valid_updates.append((cell_range, rating))

    logging.info(f"Valid updates: {len(valid_updates)} out of {len(formatting_updates)}")

    if not valid_updates:
        logging.warning("No valid formatting updates after filtering.")
        return

    try:
        format_ranges = []
        for cell_range, rating in valid_updates:
            bg_color = get_rating_color(rating)
            cell_format = CellFormat(
                backgroundColor=bg_color,
                textFormat=TextFormat(foregroundColor=Color(0, 0, 0))  # Черный текст
            )
            format_ranges.append((cell_range, cell_format))
            logging.debug(f"Prepared formatting for cell {cell_range} with rating {rating} and color {bg_color}")

        if format_ranges:
            logging.info(f"Applying {len(format_ranges)} formatting requests")
            format_cell_ranges(sheet, format_ranges)
            logging.info(f"Successfully applied formatting to {len(format_ranges)} cells.")
    except gspread.exceptions.APIError as e:
        logging.error(f"Error during batch formatting of cells: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during batch formatting: {e}")
        logging.error(traceback.format_exc())

def get_rating_color(rating):
    logging.debug(f"Getting color for rating: {rating}")
    if rating in ['N/A', 'Not Found', None, '']:
        return Color(0.5, 0.5, 0.5)  # Серый цвет для N/A или Not Found
    try:
        rating_value = float(rating)
        if rating_value >= 4.4:
            return Color(0, 1, 0)  # Зеленый
        elif 4.3 <= rating_value < 4.4:
            return Color(1, 1, 0)  # Желтый
        else:
            return Color(1, 0, 0)  # Красный
    except ValueError:
        logging.warning(f"Unable to convert rating to float: {rating}")
        return Color(1, 1, 1)  # Белый (если не удалось преобразовать рейтинг)



def log_formatting(row, col, rating, color):
    logging.info(f"Форматирование ячейки {row}{col}: Рейтинг = {rating}, Цвет = {color}")

def get_column_letter(col_num):
    """Преобразует номер столбца в буквенное обозначение (A, B, ..., Z, AA, AB, ...)"""
    letters = ''
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters

def handle_quota_exceeded():
    logging.warning("Превышена квота на запись. Ожидание восстановления квоты...")
    time.sleep(300)  # Ожидание 5 минут перед повторной попыткой


def update_google_sheets(data, client, tz):
    MAX_COLUMNS = 18278
    max_retries = 5
    base_delay = 1
    processed_products = set()

    logging.info(f"Начало обновления Google Sheets. Получено {len(data)} элементов данных.")

    try:
        # Открываем Google Sheets
        spreadsheet = retry_with_backoff(lambda: client.open_by_key('1Oadv5vT9mF3KmesPTmmbGiTrXDTRz6VEf9D_bTTYCI8'), is_read_operation=True)
        structured_sheet = retry_with_backoff(lambda: spreadsheet.worksheet("Customer reviews"), is_read_operation=True)

        all_values = retry_with_backoff(lambda: structured_sheet.get_all_values(), is_read_operation=True)
        current_date = datetime.now(tz).strftime("%d.%m.%Y")

        # Проверяем, есть ли данные на листе
        is_empty_sheet = not all_values or len(all_values) <= 1
        logging.info(f"Статус листа: {'пустой' if is_empty_sheet else 'содержит данные'}")

        if is_empty_sheet:
            # Инициализация пустого листа
            headers = ["Country", "Category", "Parent", "Parameter", current_date]
            retry_with_backoff(
                lambda: structured_sheet.clear(),
                is_read_operation=False
            )
            retry_with_backoff(
                lambda: structured_sheet.update('A1', [headers], value_input_option='USER_ENTERED'),
                is_read_operation=False
            )
            existing_asins = {}
            current_column = 5  # E колонка для первой даты
        else:
            # Получаем существующие заголовки и добавляем новую дату, если ее нет
            headers = all_values[0]
            if current_date not in headers:
                headers.append(current_date)
                retry_with_backoff(
                    lambda: structured_sheet.update('A1', [headers], value_input_option='USER_ENTERED'),
                    is_read_operation=False
                )
            current_column = headers.index(current_date) + 1  # Столбец для новой даты
            
            # Создаем словарь существующих ASIN'ов и их позиций
            existing_asins = {}
            current_row = 2
            while current_row < len(all_values):
                row = all_values[current_row - 1]
                if row[2]:  # Если есть ASIN в столбце C
                    asin = row[2].split('"')[-2] if '"' in row[2] else row[2]  # Извлекаем ASIN из HYPERLINK
                    # Сохраняем позиции всех 6 строк для каждого ASIN
                    existing_asins[asin] = {
                        'start_row': current_row,
                        'rows': {
                            'Rating': current_row,
                            'Number of Reviews': current_row + 1,
                            '5': current_row + 2,
                            '4': current_row + 3,
                            '3': current_row + 4,
                            '2': current_row + 5,
                            '1': current_row + 6
                        }
                    }
                    current_row += 7  # Переходим к следующему блоку из 7 строк
                else:
                    current_row += 1

        if current_column > MAX_COLUMNS:
            logging.error(f"Превышен лимит столбцов: {MAX_COLUMNS}.")
            return

        # Подготовка обновлений
        updates = []
        rating_cells = []  # Для хранения ячеек с рейтингами

        for product in data:
            asin = product['ASIN'].strip().upper()
            parameters = [
                ("Rating", product.get('Rating', 'N/A')),
                ("Number of Reviews", product.get('Number of Reviews', 'N/A')),
                 ("5", product.get('five_star', {}).get('Reviews with text', '0')),
                ("4", product.get('four_star', {}).get('Reviews with text', '0')),
                 ("3", product.get('three_star', {}).get('Reviews with text', '0')),
                ("2", product.get('two_star', {}).get('Reviews with text', '0')),
                ("1", product.get('one_star', {}).get('Reviews with text', '0'))
            ]
            
            if asin in existing_asins:
                # Обновляем существующие строки
                asin_info = existing_asins[asin]
                for param_name, param_value in parameters:
                    row_number = asin_info['rows'][param_name]
                    
                    # Обработка рейтинга
                    if param_name == "Rating" and param_value != 'N/A':
                        try:
                            param_value = f"{float(param_value):.1f}"
                            rating_cells.append((f'{gspread.utils.rowcol_to_a1(row_number, current_column)}', param_value))
                        except ValueError:
                            param_value = 'N/A'
                    
                    # Добавляем обновление для текущего параметра
                    updates.append({
                        'range': f'{gspread.utils.rowcol_to_a1(row_number, current_column)}',
                        'values': [[param_value]]
                    })
            else:
                # Добавляем новые строки для новых ASIN
                next_row = len(all_values) + 1
                asin_link = f'=HYPERLINK("{product["Product URL"]}"; "{asin}")'
                
                for i, (param_name, param_value) in enumerate(parameters):
                    if param_name == "Rating" and param_value != 'N/A':
                        try:
                            param_value = f"{float(param_value):.1f}"
                            rating_cells.append((f'{gspread.utils.rowcol_to_a1(next_row + i, current_column)}', param_value))
                        except ValueError:
                            param_value = 'N/A'
                    
                    row = [
                        '',  # Country пусто
                        '',  # Category пусто
                        asin_link if i == 0 else '',  # ASIN только для первой строки
                        param_name,
                        param_value
                    ]
                    updates.append({
                        'range': f'A{next_row + i}:{gspread.utils.rowcol_to_a1(next_row + i, current_column)}',
                        'values': [row]
                    })
                all_values.extend([['']*len(headers)] * len(parameters))  # Добавляем место для новых строк

        # Применяем обновления батчами
        batch_size = 10
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            try:
                retry_with_backoff(
                    lambda: structured_sheet.batch_update(batch, value_input_option='USER_ENTERED'),
                    is_read_operation=False
                )
                logging.info(f"Успешно обновлен батч данных: {len(batch)} обновлений")
            except Exception as e:
                logging.error(f"Ошибка при обновлении батча данных: {str(e)}")
                continue

        # Применяем форматирование для рейтингов
        if rating_cells:
            batch_format_cells(structured_sheet, rating_cells)

        logging.info(f"Обновление завершено. Обработано продуктов: {len(data)}")

    except gspread.exceptions.APIError as e:
        if 'quota exceeded' in str(e).lower():
            handle_quota_exceeded()
        else:
            logging.error(f"Ошибка API: {e}")
            raise
    except Exception as e:
        logging.error(f"Неожиданная ошибка: {e}")
        raise





MAX_MESSAGE_LENGTH = 4096

def send_split_telegram_message(bot, chat_id, message):
    """Отправляет сообщение в Telegram, разбивая его на части, если оно слишком длинное."""
    if len(message) <= MAX_MESSAGE_LENGTH:
        bot.send_message(chat_id, message, parse_mode='HTML')
    else:
        # Разбиваем сообщение на части и отправляем по частям
        parts = [message[i:i + MAX_MESSAGE_LENGTH] for i in range(0, len(message), MAX_MESSAGE_LENGTH)]
        for part in parts:
            bot.send_message(chat_id, part, parse_mode='HTML')

# Создаем словарь для хранения истории рейтингов для каждого ASIN
rating_history = defaultdict(list)  # Используем defaultdict для упрощения работы с отсутствующими ключами

def check_low_rating_for_multiple_days(asin, current_rating, threshold, history_days=3):
    """
    Проверяет, ниже ли рейтинг текущего продукта в течение нескольких дней подряд.
    :param asin: ASIN продукта.
    :param current_rating: Текущий рейтинг продукта.
    :param threshold: Минимально допустимый рейтинг.
    :param history_days: Количество дней для проверки (по умолчанию 3).
    :return: True, если рейтинг ниже порога несколько дней подряд, иначе False.
    """
    if current_rating is None or current_rating == 'N/A':
        return False
    
    try:
        current_rating = float(current_rating)
    except ValueError:
        return False
    
    # Добавляем текущий рейтинг в историю
    rating_history[asin].append(current_rating)
    
    # Удаляем старые записи, если их больше чем необходимо для проверки
    if len(rating_history[asin]) > history_days:
        rating_history[asin] = rating_history[asin][-history_days:]

    # Проверяем, ниже ли рейтинг порога несколько дней подряд
    return all(r < threshold for r in rating_history[asin])


def send_telegram_message(bot, chat_id, products_info, config, tz):
    """Отправляет одно сообщение с завершением задачи и продуктами с рейтингом ниже минимально приемлемого."""
   
    message = (
        "✅ <b>Задача сбора данных выполнена и добавлена.</b>\n"
        "⭐️ <b>Контроль количества отзывов по звездам</b>\n"
        f"Дата: {datetime.now(tz).strftime('%d.%m.%Y')}.\n\n"
    )

    min_acceptable_rating = float(config.get('min_acceptable_rating', 4.3))
    history_days = 3  # количество дней для проверки

    # Фильтруем продукты с рейтингом ниже минимально приемлемого
    low_rating_products = [
        p for p in products_info 
        if p['Rating'] != 'Not Found' 
        and p['Rating'] != 'N/A' 
        and p['Rating'] != ''
        and check_low_rating_for_multiple_days(p['ASIN'], p['Rating'], min_acceptable_rating, history_days)
    ]

    logging.info(f"Найдено продуктов с рейтингом ниже {min_acceptable_rating} в течение {history_days} дней: {len(low_rating_products)}")
    logging.debug(f"Продукты с низким рейтингом: {low_rating_products}")

    if low_rating_products:
        for product in low_rating_products: 
            asin = product['ASIN']
            rating = product['Rating']
            product_url = product['Product URL']
            message += (
                "⚠️ <b>Внимание!</b>\n"
                f"ASIN: <a href='{product_url}'>{asin}</a>\n"
                f"Рейтинг: <b>{rating}</b> - ниже минимально приемлемого ({min_acceptable_rating}) в течение {history_days} дней!\n\n"
            )
    else:
        message += f"Продуктов с рейтингом ниже {min_acceptable_rating} не найдено."

    try:
        # Используем функцию для отправки длинного сообщения
        send_split_telegram_message(bot, chat_id, message)
        logging.info("Уведомление о завершении задачи и низких рейтингах успешно отправлено в Telegram.")
    except Exception as e:
        logging.error(f"Ошибка при отправке сообщения: {str(e)}")




def get_credentials_path():
    """Функция для поиска пути к файлу учетных данных."""
    possible_paths = [
        os.path.join(os.path.expanduser('~'), 'Downloads', 'maximumstores53-24d4ef8c1298.json'),
        os.path.join(os.getcwd(), 'maximumstores53-24d4ef8c1298.json'),
        'maximumstores53-24d4ef8c1298.json'
    ]

    credentials_path = None
    for path in possible_paths:
        if os.path.exists(path):
            credentials_path = path
            logging.info(f"Файл учетных данных найден по пути: {credentials_path}")
            break

    if credentials_path is None:
        logging.error("Файл учетных данных не найден ни в одном из ожидаемых мест.")
        raise FileNotFoundError("Файл учетных данных не найден.")
    
    return credentials_path

def main():
    try:
        # Настройка доступа к Google Sheets для конфигурации
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_path = get_credentials_path()  # Используем функцию для поиска пути
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
        client = gspread.authorize(creds)
        config_spreadsheet = client.open_by_key('1Oadv5vT9mF3KmesPTmmbGiTrXDTRz6VEf9D_bTTYCI8')
        config_sheet = config_spreadsheet.worksheet("Config")
        config = read_config(config_sheet)

        # Извлекаем параметры
        update_hour = int(config.get('update_time_hour', 8))
        update_minute = int(config.get('update_time_minute', 10))
        timezone_str = config.get('timezone', 'Europe/Kiev')
        product_urls = config.get('product_urls', [])
        min_acceptable_rating = float(config.get('min_acceptable_rating', 4.3))

        logging.debug(f"Configuration loaded: update_hour={update_hour}, update_minute={update_minute}, timezone={timezone_str}, min_acceptable_rating={min_acceptable_rating}")

        # Настройка Telegram бота
        bot = telebot.TeleBot(config['telegram_bot_token'])
        CHAT_ID = config['telegram_chat_id']

        # Настройка часового пояса
        try:
            tz = pytz.timezone(timezone_str)
        except pytz.UnknownTimeZoneError:
            logging.error(f"Неизвестный часовой пояс: {timezone_str}. Используем Europe/Kiev.")
            tz = pytz.timezone('Europe/Kiev')

        logging.info(f"Часовой пояс установлен на {timezone_str}.")

        def perform_update():
            logging.info("Начало обновления данных при запуске скрипта.")
            results = []

            for url in product_urls:
                logging.debug(f"Scraping URL: {url}")
                product_info = scrape_amazon_product(url, config['oxylabs_username'], config['oxylabs_password'], tz)

                if product_info:
                    results.append(product_info)
                    logging.info(f"Успешно получены данные для {url}")
                else:
                    logging.warning(f"Не удалось получить данные для {url}")

            if results:
                logging.debug(f"Updating Google Sheets with {len(results)} results.")
                update_google_sheets(results, client, tz)
                logging.debug("Sending Telegram message.")
                send_telegram_message(bot, CHAT_ID, results, config, tz)
            else:
                logging.warning("Нет данных для выгрузки в Google Таблицу.")
                bot.send_message(CHAT_ID, "✅ <b>Задача сбора данных выполнена, но данных нет.</b>", parse_mode='HTML')
                logging.info("Уведомление о выполнении задачи без данных успешно отправлено в Telegram.")

            logging.info(f"Обновление данных завершено в {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')}.")

        perform_update()

        while True:
            now = datetime.now(tz)
            next_run = now.replace(hour=update_hour, minute=update_minute, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
            sleep_seconds = (next_run - now).total_seconds()
            logging.info(f"Следующий запуск запланирован на {next_run.strftime('%Y-%m-%d %H:%M:%S')} (через {sleep_seconds:.0f} секунд).")
            time.sleep(sleep_seconds)

            logging.info("Начало обновления данных по расписанию.")
            results = []

            for url in product_urls:
                logging.debug(f"Scraping URL: {url}")
                product_info = scrape_amazon_product(url, config['oxylabs_username'], config['oxylabs_password'], tz)

                if product_info:
                    results.append(product_info)
                    logging.info(f"Успешно получены данные для {url}")
                else:
                    logging.warning(f"Не удалось получить данные для {url}")

            if results:
                logging.debug(f"Updating Google Sheets with {len(results)} results.")
                update_google_sheets(results, client, tz)
                logging.debug("Sending Telegram message.")
                send_telegram_message(bot, CHAT_ID, results, config, tz)
            else:
                logging.warning("Нет данных для выгрузки в Google Таблицу.")

            logging.info(f"Обновление данных завершено в {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')}.")

    except Exception as e:
        logging.error(f"Ошибка в основной функции: {e}")
        logging.debug(traceback.format_exc())
        time.sleep(60)  # Ждем минуту перед повторной попыткой

if __name__ == '__main__':
    main()
