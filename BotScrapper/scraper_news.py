import time
import random
import mysql.connector
from datetime import datetime, timedelta
import pytz
import requests
from bs4 import BeautifulSoup
import re
try:
    from deep_translator import GoogleTranslator
    TRANSLATOR_AVAILABLE = True
except ImportError:
    TRANSLATOR_AVAILABLE = False
    print("Module deep_translator non disponible - les traductions seront désactivées")

# --- Configuration de la base de données ---
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'uD4kHXXn2W9gFPb',
    'database': 'BotScrappingNews'
}

try:
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    print("Connexion à la base BotScrappingNews réussie !")
except mysql.connector.Error as err:
    print(f"Erreur de connexion à la BDD: {err}")
    exit(1)

# --- Création/mise à jour de la table news ---
create_table_query = """
CREATE TABLE IF NOT EXISTS news (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    title_fr VARCHAR(500),
    link VARCHAR(1000) NOT NULL,
    source VARCHAR(255) NOT NULL,
    date_time DATETIME NOT NULL,
    ticker VARCHAR(100),
    category VARCHAR(100),
    content TEXT,
    content_fr TEXT,
    published TINYINT(1) NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
"""
try:
    # Vérifier si la colonne title_fr existe
    cursor.execute("SHOW COLUMNS FROM news LIKE 'title_fr'")
    if not cursor.fetchone():
        cursor.execute("ALTER TABLE news ADD COLUMN title_fr VARCHAR(500)")
        print("Colonne title_fr ajoutée à la table news.")
    
    # Vérifier si la colonne content_fr existe
    cursor.execute("SHOW COLUMNS FROM news LIKE 'content_fr'")
    if not cursor.fetchone():
        cursor.execute("ALTER TABLE news ADD COLUMN content_fr TEXT")
        print("Colonne content_fr ajoutée à la table news.")
    
    cursor.execute(create_table_query)
    connection.commit()
    print("Table news prête ou déjà existante.")
except mysql.connector.Error as err:
    print(f"Erreur lors de la création/mise à jour de la table: {err}")

# --- Création d'un index pour eviter les doublons ---
try:
    cursor.execute("SHOW INDEX FROM news WHERE Key_name = 'idx_link'")
    if not cursor.fetchone():
        cursor.execute("CREATE UNIQUE INDEX idx_link ON news(link)")
        connection.commit()
        print("Index unique sur le lien créé.")
except mysql.connector.Error as err:
    print(f"Erreur lors de la création de l'index: {err}")

# --- Fonction pour traduire le texte en français ---
def translate_to_french(text):
    if not text or not TRANSLATOR_AVAILABLE:
        return None
    try:
        # Limite de caractères par requête de traduction
        max_chars = 5000
        
        # Si le texte est trop long, le diviser en parties
        if len(text) > max_chars:
            parts = [text[i:i+max_chars] for i in range(0, len(text), max_chars)]
            translated_parts = []
            
            for part in parts:
                translator = GoogleTranslator(source='en', target='fr')
                translated_part = translator.translate(part)
                translated_parts.append(translated_part)
            
            return ''.join(translated_parts)
        else:
            translator = GoogleTranslator(source='en', target='fr')
            return translator.translate(text)
    except Exception as e:
        print(f"Erreur lors de la traduction: {e}")
        return None

# --- Fonction pour obtenir des détails supplémentaires d'une news ---
def get_news_details(url):
    """
    Essaie d'obtenir le contenu d'une news à partir de son URL.
    Retourne un dictionnaire avec le contenu et autres détails.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return {"content": None, "category": None}
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Essayer de trouver le contenu principal (cela varie selon les sites)
        content = None
        article_body = soup.find('article') or soup.find(class_=re.compile('article|content|story'))
        if article_body:
            paragraphs = article_body.find_all('p')
            if paragraphs:
                content = ' '.join([p.text.strip() for p in paragraphs])
        
        # Essayer de trouver la catégorie
        category = None
        category_elem = soup.find(class_=re.compile('category|tag|topic|breadcrumb'))
        if category_elem:
            category = category_elem.text.strip()
        
        return {
            "content": content[:500] + "..." if content and len(content) > 500 else content,
            "category": category
        }
    except Exception as e:
        print(f"Erreur lors de l'extraction des détails de news: {e}")
        return {"content": None, "category": None}

# --- Méthode très directe pour extraire les news de Finviz ---
def scrape_news():
    try:
        # URL de la page des news de Finviz
        url = "https://finviz.com/news.ashx"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        print(f"Téléchargement de la page {url}...")
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Erreur HTTP: {response.status_code}")
            return []
        
        print("Analyse de la page HTML...")
        # Sauvegarder le HTML pour le débogage
        with open("finviz_news.html", "w", encoding="utf-8") as f:
            f.write(response.text)
        print("Page HTML sauvegardée dans finviz_news.html pour débogage")
        
        # Utiliser BeautifulSoup pour l'analyse
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Trouver toutes les nouvelles dans la section news
        news_list = []
        
        # Méthode 1: Chercher les liens dans les tables de news
        news_links = soup.select('table.table-fixed a')
        if news_links:
            print(f"Méthode 1: Trouvé {len(news_links)} liens de news")
            
            for link in news_links:
                title = link.text.strip()
                url = link.get('href')
                
                # Trouver l'élément parent tr
                tr_parent = link.find_parent('tr')
                if not tr_parent:
                    continue
                
                # Chercher les spans pour source et date
                source = "Finviz"
                date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                spans = tr_parent.select('span')
                if len(spans) >= 2:
                    source = spans[0].text.strip()
                    date_str = spans[1].text.strip()
                
                # Extraire le ticker si présent
                ticker_match = re.search(r'\(([A-Z]+)\)', title)
                ticker = ticker_match.group(1) if ticker_match else ""
                
                # Créer l'entrée de news
                news_item = {
                    'Title': title,
                    'Link': url,
                    'Source': source,
                    'Time': date_str,
                    'parsed_date': datetime.now(),
                    'Ticker': ticker
                }
                
                # Traduire le titre
                if TRANSLATOR_AVAILABLE:
                    news_item['Title_fr'] = translate_to_french(title)
                else:
                    news_item['Title_fr'] = None
                
                news_list.append(news_item)
                print(f"News trouvée: {title} - Source: {source}")
        
        # Méthode 2: Chercher toutes les lignes de nouvelles directement
        if not news_list:
            print("Méthode 2: Recherche directe des nouvelles...")
            
            # Trouver toutes les lignes qui contiennent des nouvelles
            rows = soup.find_all('tr')
            for row in rows:
                try:
                    # Une ligne de news contient typiquement un lien et des spans
                    link = row.find('a')
                    if not link:
                        continue
                    
                    title = link.text.strip()
                    url = link.get('href')
                    
                    if not title or not url:
                        continue
                    
                    # Chercher les spans (généralement pour source et date)
                    spans = row.find_all('span')
                    source = "Finviz"
                    date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    if len(spans) >= 2:
                        source = spans[0].text.strip()
                        date_str = spans[1].text.strip()
                    
                    # Extraire le ticker si présent
                    ticker_match = re.search(r'\(([A-Z]+)\)', title)
                    ticker = ticker_match.group(1) if ticker_match else ""
                    
                    # Créer l'entrée de news
                    news_item = {
                        'Title': title,
                        'Link': url,
                        'Source': source,
                        'Time': date_str,
                        'parsed_date': datetime.now(),
                        'Ticker': ticker
                    }
                    
                    # Traduire le titre
                    if TRANSLATOR_AVAILABLE:
                        news_item['Title_fr'] = translate_to_french(title)
                    else:
                        news_item['Title_fr'] = None
                    
                    news_list.append(news_item)
                    print(f"News trouvée (méthode 2): {title} - Source: {source}")
                except Exception as e:
                    print(f"Erreur lors du traitement d'une ligne: {e}")
        
        # Méthode 3: Utiliser les sélecteurs CSS directs pour les news
        if not news_list:
            print("Méthode 3: Recherche via sélecteurs CSS...")
            
            # Sélecteur direct pour la section des news
            news_selectors = [
                'td.nn-tab-link', 
                '.news-link', 
                '.news-left',
                'table td a'
            ]
            
            for selector in news_selectors:
                links = soup.select(selector)
                print(f"Sélecteur '{selector}': {len(links)} éléments trouvés")
                
                for link in links:
                    try:
                        title = link.text.strip()
                        url = link.get('href')
                        
                        if not title or not url:
                            continue
                        
                        # Créer une entrée basique
                        news_item = {
                            'Title': title,
                            'Link': url,
                            'Source': "Finviz",
                            'Time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'parsed_date': datetime.now(),
                            'Ticker': ""
                        }
                        
                        # Traduire le titre
                        if TRANSLATOR_AVAILABLE:
                            news_item['Title_fr'] = translate_to_french(title)
                        else:
                            news_item['Title_fr'] = None
                        
                        news_list.append(news_item)
                        print(f"News trouvée (méthode 3): {title}")
                    except Exception as e:
                        print(f"Erreur lors du traitement d'un lien: {e}")
        
        # Méthode 4: Recherche très basique de tous les liens
        if not news_list:
            print("Méthode 4: Recherche de tous les liens pertinents...")
            
            # Trouver tous les liens
            all_links = soup.find_all('a')
            for link in all_links:
                try:
                    url = link.get('href')
                    title = link.text.strip()
                    
                    # Filtrer les liens vides ou trop courts
                    if not url or not title or len(title) < 10:
                        continue
                    
                    # Filtrer pour n'avoir que des liens probables de news
                    if '/news/' in url or '.cnbc.com' in url or '.reuters.com' in url or 'marketwatch.com' in url:
                        news_item = {
                            'Title': title,
                            'Link': url,
                            'Source': "Finviz",
                            'Time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'parsed_date': datetime.now(),
                            'Ticker': ""
                        }
                        
                        # Traduire le titre
                        if TRANSLATOR_AVAILABLE:
                            news_item['Title_fr'] = translate_to_french(title)
                        else:
                            news_item['Title_fr'] = None
                        
                        news_list.append(news_item)
                        print(f"News trouvée (méthode 4): {title}")
                except Exception as e:
                    print(f"Erreur lors du traitement d'un lien général: {e}")
        
        # Ajouter des détails supplémentaires pour certaines news
        for news in random.sample(news_list, min(len(news_list), 3)):
            try:
                details = get_news_details(news['Link'])
                news['content'] = details['content']
                news['category'] = details['category']
                
                # Traduire le contenu
                if news['content'] and TRANSLATOR_AVAILABLE:
                    news['content_fr'] = translate_to_french(news['content'])
                else:
                    news['content_fr'] = None
            except Exception as e:
                print(f"Erreur lors de la récupération des détails: {e}")
                news['content'] = None
                news['content_fr'] = None
                news['category'] = None
        
        print(f"Total de news trouvées: {len(news_list)}")
        return news_list
    
    except Exception as e:
        print(f"Erreur générale lors du scraping: {e}")
        return []

# --- Fonction pour vérifier si une news existe déjà dans la base de données ---
def check_news_exists(title, link):
    """
    Vérifie si une news avec le même titre ou lien existe déjà dans la base de données.
    Retourne True si elle existe, False sinon.
    """
    try:
        query = """
        SELECT COUNT(*) as count 
        FROM news 
        WHERE title = %s OR link = %s
        """
        cursor.execute(query, (title, link))
        result = cursor.fetchone()
        
        # Si le résultat est supérieur à 0, la news existe déjà
        return result['count'] > 0 if isinstance(result, dict) else result[0] > 0
    except mysql.connector.Error as err:
        print(f"Erreur lors de la vérification de l'existence de la news: {err}")
        return False  # En cas d'erreur, on considère que la news n'existe pas

# --- Fonction d'insertion des news en BDD ---
def save_news_to_db(news_list):
    insert_query = """
    INSERT IGNORE INTO news 
    (title, title_fr, link, source, date_time, ticker, category, content, content_fr, published)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
    """
    count = 0
    skipped = 0
    
    for news in news_list:
        try:
            # Vérifier si la news existe déjà dans la BDD
            if check_news_exists(news['Title'], news['Link']):
                skipped += 1
                continue
                
            dt_str = news['parsed_date'].strftime("%Y-%m-%d %H:%M:%S")
            ticker = news.get('Ticker', '')
            
            cursor.execute(insert_query, (
                news['Title'],
                news.get('Title_fr'),
                news['Link'],
                news['Source'],
                dt_str,
                ticker,
                news.get('category'),
                news.get('content'),
                news.get('content_fr')
            ))
            count += 1
        except mysql.connector.Error as err:
            if err.errno == 1062:  # Duplicate entry for key
                skipped += 1
            else:
                print(f"Erreur d'insertion: {err}")
    
    connection.commit()
    print(f"{count} nouvelles news insérées dans la base de données.")
    print(f"{skipped} news ignorées car déjà présentes dans la base de données.")

# --- Fonction de suppression des news datant de plus de 48 heures ---
def delete_old_news():
    delete_query = """
    DELETE FROM news
    WHERE date_time < (NOW() - INTERVAL 48 HOUR)
    """
    try:
        cursor.execute(delete_query)
        nb_deleted = cursor.rowcount
        connection.commit()
        if nb_deleted > 0:
            print(f"{nb_deleted} news de plus de 48h supprimées.")
    except mysql.connector.Error as err:
        print(f"Erreur lors de la suppression des anciennes news: {err}")

# --- Boucle principale ---
if __name__ == "__main__":
    # Installation du module deep_translator si nécessaire
    if not TRANSLATOR_AVAILABLE:
        try:
            import pip
            pip.main(['install', 'deep_translator'])
            print("Module deep_translator installé avec succès.")
            from deep_translator import GoogleTranslator
            TRANSLATOR_AVAILABLE = True
        except Exception as e:
            print(f"Erreur lors de l'installation du module: {e}")
    
    while True:
        print("Début du scraping des news Finviz...")
        news_list = scrape_news()
        if news_list:
            save_news_to_db(news_list)
        else:
            print("Aucune news récupérée ou erreur lors du scraping.")
        
        # Supprimer les news datant de plus de 48 heures
        delete_old_news()

        # Pause aléatoire entre 15 et 30 minutes
        interval = random.randint(900, 1800)
        m, s = divmod(interval, 60)
        print(f"Pause de {m} min {s} sec avant la prochaine exécution.")
        time.sleep(interval)
