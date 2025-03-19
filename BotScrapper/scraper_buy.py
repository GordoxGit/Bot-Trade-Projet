import time
import random
import mysql.connector
from datetime import datetime, timedelta
import pytz
from finvizfinance.insider import Insider

# --- Configuration de la base de données ---
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'uD4kHXXn2W9gFPb',
    'database': 'BotScrappingBuy'
}

try:
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    print("Connexion à la base BotScrappingBuy réussie !")
except mysql.connector.Error as err:
    print(f"Erreur de connexion à la BDD: {err}")
    exit(1)

# --- Création/mise à jour de la table quotes_buy ---
create_table_query = """
CREATE TABLE IF NOT EXISTS quotes_buy (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    owner VARCHAR(255) NOT NULL,
    relationship VARCHAR(100) NOT NULL,
    trade_date VARCHAR(50) NOT NULL,
    transaction VARCHAR(100) NOT NULL,
    cost_value VARCHAR(100) NOT NULL,
    sec_form4 DATETIME NOT NULL,
    published TINYINT(1) NOT NULL DEFAULT 0,
    saved TINYINT(1) NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
"""
try:
    cursor.execute(create_table_query)
    connection.commit()
    print("Table quotes_buy prête ou déjà existante.")
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

# --- Fonction de scraping via finvizfinance ---
def scrape_transactions():
    try:
        # On utilise l'option 'latest' et on filtrera les BUY ensuite
        insider = Insider(option='latest')
        df = insider.get_insider()  # Retourne un DataFrame pandas
    except Exception as e:
        print(f"Erreur lors du scraping via finvizfinance : {e}")
        return []

    if df is None or df.empty:
        print("Aucune donnée retournée par finvizfinance.")
        return []

    # Filtrer uniquement les transactions de type "Buy"
    df = df[df['Transaction'].str.contains('Buy', case=False)]
    
    if df.empty:
        print("Aucune transaction BUY trouvée.")
        return []

    # Afficher les colonnes pour debug
    print("Colonnes retournées :", df.columns.tolist())
    now = datetime.now(pytz.utc)
    records = df.to_dict('records')
    transactions = []
    for r in records:
        # Utilise la colonne "SEC Form 4" pour obtenir la date et l'heure
        sec_form4_str = r.get("SEC Form 4", "")
        parsed_dt = parse_sec_form4_date(sec_form4_str)
        if not parsed_dt:
            continue
        # Ne garder que si la transaction est apparue dans les 10 dernières minutes
        if (now - parsed_dt) > timedelta(minutes=10):
            continue
        cost_value = f"{r.get('Cost', '')} {r.get('Value ($)', '')}".strip()
        transactions.append({
            "ticker": r.get("Ticker", ""),
            "owner": r.get("Owner", ""),
            "relationship": r.get("Relationship", ""),
            "trade_date": r.get("Date", ""),  # on conserve le format original pour affichage
            "transaction": r.get("Transaction", ""),
            "cost_value": cost_value,
            "sec_form4_dt": parsed_dt
        })
    return transactions

# --- Fonction d'insertion en BDD ---
def save_transactions_to_db(transactions):
    insert_query = """
    INSERT INTO quotes_buy 
    (ticker, owner, relationship, trade_date, transaction, cost_value, sec_form4, published, saved)
    VALUES (%s, %s, %s, %s, %s, %s, %s, 0, 0)
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
                dt_str
            ))
            count += 1
        except mysql.connector.Error as err:
            print(f"Erreur d'insertion: {err}")
    connection.commit()
    print(f"{count} transactions BUY insérées dans la base de données.")

# --- Fonction de suppression des transactions non sauvegardées datant de plus de 10 minutes ---
def delete_old_unsaved_transactions():
    delete_query = """
    DELETE FROM quotes_buy
    WHERE saved = 0
      AND sec_form4 < (NOW() - INTERVAL 10 MINUTE)
    """
    try:
        cursor.execute(delete_query)
        nb_deleted = cursor.rowcount
        connection.commit()
        if nb_deleted > 0:
            print(f"{nb_deleted} transactions BUY non sauvegardées (plus de 10 min) supprimées.")
    except mysql.connector.Error as err:
        print(f"Erreur lors de la suppression: {err}")

# --- Boucle principale ---
if __name__ == "__main__":
    while True:
        print("Début du scraping des transactions BUY Finviz Insider Trading en se basant sur SEC Form 4 (<10 min)...")
        transactions = scrape_transactions()
        if transactions:
            save_transactions_to_db(transactions)
        else:
            print("Aucune transaction BUY trouvée dans les 10 dernières minutes ou parsing impossible.")
        
        # Supprimer les transactions non sauvegardées datant de plus de 10 minutes
        delete_old_unsaved_transactions()

        # Pause aléatoire entre 4 et 12 minutes (en rejetant les multiples de 60)
        interval = random.randint(240, 720)
        while interval % 60 == 0:
            interval = random.randint(240, 720)
        m, s = divmod(interval, 60)
        print(f"Pause de {m} min {s} sec avant la prochaine exécution.")
        time.sleep(interval)
