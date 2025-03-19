import time
import random
import mysql.connector
from datetime import datetime, timedelta
import pytz
from finvizfinance.insider import Insider
import requests
from bs4 import BeautifulSoup
import pandas as pd

# --- Configuration de la base de données ---
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'uD4kHXXn2W9gFPb',
    'database': 'BotScrappingBigBuy'
}

try:
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    print("Connexion à la base BotScrappingBigBuy réussie !")
except mysql.connector.Error as err:
    print(f"Erreur de connexion à la BDD: {err}")
    exit(1)

# --- Création/mise à jour de la table quotes_bigbuy ---
create_table_query = """
CREATE TABLE IF NOT EXISTS quotes_bigbuy (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    owner VARCHAR(255) NOT NULL,
    relationship VARCHAR(100) NOT NULL,
    trade_date VARCHAR(50) NOT NULL,
    transaction VARCHAR(100) NOT NULL,
    cost_value VARCHAR(100) NOT NULL,
    sec_form4 DATETIME NOT NULL,
    transaction_value DECIMAL(20, 2) NOT NULL,
    published TINYINT(1) NOT NULL DEFAULT 0,
    saved TINYINT(1) NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
"""
try:
    cursor.execute(create_table_query)
    connection.commit()
    print("Table quotes_bigbuy prête ou déjà existante.")
except mysql.connector.Error as err:
    print(f"Erreur lors de la création/mise à jour de la table: {err}")

# --- Fonction pour parser le champ SEC Form 4 ---
def parse_sec_form4_date(date_str):
    """
    Parse une date de Finviz au format "Mar 17 05:14 PM".
    Finviz utilise le fuseau horaire Eastern.
    On ajoute l'année courante, on localise en ET et on convertit en UTC.
    """
    try:
        # On parse la date sans année
        dt_naive = datetime.strptime(date_str, "%b %d %I:%M %p")
        # On ajoute l'année courante
        now = datetime.now()
        dt_naive = dt_naive.replace(year=now.year)
        # Localiser en fuseau Eastern
        et = pytz.timezone("US/Eastern")
        dt_et = et.localize(dt_naive)
        # Convertir en UTC
        dt_utc = dt_et.astimezone(pytz.utc)
        return dt_utc
    except ValueError as e:
        print(f"Erreur de parsing pour '{date_str}': {e}")
        return None

# --- Fonction pour extraire la valeur numérique du champ de transaction ---
def extract_transaction_value(value_str):
    """
    Extrait la valeur numérique d'une chaîne de texte représentant une valeur financière.
    Par exemple : "$1.2M" -> 1200000, "$500K" -> 500000
    """
    try:
        if not value_str or value_str == "":
            return 0
        
        # Supprimer le symbole $ et les espaces
        value_str = value_str.replace('$', '').replace(',', '').strip()
        
        # Convertir les suffixes en valeurs numériques
        if value_str.endswith('K'):
            return float(value_str[:-1]) * 1000
        elif value_str.endswith('M'):
            return float(value_str[:-1]) * 1000000
        elif value_str.endswith('B'):
            return float(value_str[:-1]) * 1000000000
        else:
            return float(value_str)
    except Exception as e:
        print(f"Erreur lors de l'extraction de la valeur de transaction: {e}")
        return 0

# --- Fonction de scraping via web scraping direct pour les grandes transactions ---
def scrape_big_transactions():
    try:
        # URL pour les transactions d'achat de grande valeur (>$100,000)
        url = "https://finviz.com/insidertrading.ashx?or=-10&tv=100000&tc=1&o=-transactionvalue"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Trouver la table contenant les données des insiders
        insider_table = soup.find('table', {'class': 'table-insider'})
        if not insider_table:
            print("Impossible de trouver la table des transactions sur la page.")
            return []
        
        # Récupérer les en-têtes
        headers = []
        header_row = insider_table.find('tr', {'class': 'table-top'})
        if header_row:
            headers = [th.text.strip() for th in header_row.find_all('td')]
        
        # Récupérer les lignes de données
        rows = insider_table.find_all('tr', {'class': 'insider-buy-row-1'}) + insider_table.find_all('tr', {'class': 'insider-buy-row-2'})
        
        transactions = []
        now = datetime.now(pytz.utc)
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= len(headers):
                data = {}
                for i, cell in enumerate(cells):
                    if i < len(headers):
                        data[headers[i]] = cell.text.strip()
                
                # Vérifier que nous avons les colonnes nécessaires
                required_cols = ['Ticker', 'Owner', 'Relationship', 'Date', 'Transaction', 'Cost', 'Value', 'SEC Form 4']
                if not all(col in data for col in required_cols):
                    continue
                
                # Analyser la date SEC Form 4
                sec_form4_str = data.get('SEC Form 4', '')
                parsed_dt = parse_sec_form4_date(sec_form4_str)
                if not parsed_dt:
                    continue
                
                # Ne garder que si la transaction est apparue dans les 10 dernières minutes
                if (now - parsed_dt) > timedelta(minutes=10):
                    continue
                
                # Extraire la valeur de la transaction
                value_str = data.get('Value', '0')
                transaction_value = extract_transaction_value(value_str)
                
                # Ne garder que les transactions supérieures à $100,000
                if transaction_value < 100000:
                    continue
                
                cost_value = f"{data.get('Cost', '')} {data.get('Value', '')}".strip()
                
                transactions.append({
                    "ticker": data.get('Ticker', ''),
                    "owner": data.get('Owner', ''),
                    "relationship": data.get('Relationship', ''),
                    "trade_date": data.get('Date', ''),
                    "transaction": data.get('Transaction', ''),
                    "cost_value": cost_value,
                    "sec_form4_dt": parsed_dt,
                    "transaction_value": transaction_value
                })
        
        return transactions
        
    except Exception as e:
        print(f"Erreur lors du scraping des grandes transactions: {e}")
        return []

# --- Fonction d'insertion en BDD ---
def save_transactions_to_db(transactions):
    insert_query = """
    INSERT INTO quotes_bigbuy 
    (ticker, owner, relationship, trade_date, transaction, cost_value, sec_form4, transaction_value, published, saved)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, 0)
    """
    count = 0
    for t in transactions:
        dt_str = t["sec_form4_dt"].strftime("%Y-%m-%d %H:%M:%S")
        try:
            cursor.execute(insert_query, (
                t["ticker"],
                t["owner"],
                t["relationship"],
                t["trade_date"],
                t["transaction"],
                t["cost_value"],
                dt_str,
                t["transaction_value"]
            ))
            count += 1
        except mysql.connector.Error as err:
            print(f"Erreur d'insertion: {err}")
    connection.commit()
    print(f"{count} transactions BIG BUY insérées dans la base de données.")

# --- Fonction de suppression des transactions non sauvegardées datant de plus de 10 minutes ---
def delete_old_unsaved_transactions():
    delete_query = """
    DELETE FROM quotes_bigbuy
    WHERE saved = 0
      AND sec_form4 < (NOW() - INTERVAL 10 MINUTE)
    """
    try:
        cursor.execute(delete_query)
        nb_deleted = cursor.rowcount
        connection.commit()
        if nb_deleted > 0:
            print(f"{nb_deleted} transactions BIG BUY non sauvegardées (plus de 10 min) supprimées.")
    except mysql.connector.Error as err:
        print(f"Erreur lors de la suppression: {err}")

# --- Boucle principale ---
if __name__ == "__main__":
    while True:
        print("Début du scraping des GRANDES transactions d'achat (>$100K) sur Finviz Insider Trading en se basant sur SEC Form 4 (<10 min)...")
        transactions = scrape_big_transactions()
        if transactions:
            save_transactions_to_db(transactions)
        else:
            print("Aucune grande transaction d'achat trouvée dans les 10 dernières minutes ou parsing impossible.")
        
        # Supprimer les transactions non sauvegardées datant de plus de 10 minutes
        delete_old_unsaved_transactions()

        # Pause aléatoire entre 4 et 12 minutes (en rejetant les multiples de 60)
        interval = random.randint(240, 720)
        while interval % 60 == 0:
            interval = random.randint(240, 720)
        m, s = divmod(interval, 60)
        print(f"Pause de {m} min {s} sec avant la prochaine exécution.")
        time.sleep(interval)
