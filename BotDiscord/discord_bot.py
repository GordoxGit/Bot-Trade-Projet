import discord
import mysql.connector
import asyncio
import yfinance as yf
import matplotlib.pyplot as plt
import io
from datetime import datetime
import pytz
from finvizfinance.insider import Insider

# -------------------------------
# Configuration de la base de données (BotScrapping)
# -------------------------------
db_config_scrap = {
    'host': 'localhost',
    'user': 'root',
    'password': 'uD4kHXXn2W9gFPb',
    'database': 'BotScrapping'
}

# Fonction pour établir une connexion à la BDD
def get_db_connection():
    try:
        conn = mysql.connector.connect(**db_config_scrap)
        return conn
    except mysql.connector.Error as err:
        print(f"Erreur de connexion à BotScrapping: {err}")
        return None

# Initialisation (seulement pour vérifier au démarrage)
connection_scrap = get_db_connection()
if not connection_scrap:
    exit(1)
cursor_scrap = connection_scrap.cursor(dictionary=True)
print("Connexion initiale à BotScrapping réussie !")
connection_scrap.close()

# -------------------------------
# Configuration du bot Discord (py-cord)
# -------------------------------
TOKEN = "MTM0OTYzNDIzNjc2Mzg2NTEyOA.Gw9ZdE.j16qejGAWrZVT6HiBDPxk1XbkbBonfr1okJtNo"
CHANNEL_ID = 1349638200880267307  # Remplace par l'ID de ton salon Discord
intents = discord.Intents.default()
bot = discord.Bot(intents=intents)

# -------------------------------
# Fonction pour générer un graphique d'un ticker
# -------------------------------
def get_stock_chart(ticker: str):
    try:
        data = yf.download(ticker, period="1d", interval="5m")
        if data.empty:
            print(f"Aucune donnée trouvée pour {ticker}")
            return None
        plt.figure(figsize=(6, 4))
        plt.plot(data.index, data['Close'], label="Prix de clôture", color="green")
        plt.title(f"Graphique pour {ticker.upper()}")
        plt.xlabel("Temps")
        plt.ylabel("Prix de clôture")
        plt.legend()
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format="png")
        buf.seek(0)
        plt.close()
        return buf
    except Exception as e:
        print(f"Erreur lors de la génération du graphique pour {ticker}: {e}")
        return None

# -------------------------------
# Fonction pour créer un embed et attacher un graphique pour une transaction
# -------------------------------
def create_embed_and_file(row):
    chart_buffer = get_stock_chart(row['ticker'])
    embed = discord.Embed(
        title=f"Transaction Insider pour {row['ticker']}",
        description="Nouvelle transaction détectée sur Finviz Insider Trading",
        color=0x3498db
    )
    embed.add_field(name="Owner", value=row["owner"], inline=True)
    embed.add_field(name="Relationship", value=row["relationship"], inline=True)
    embed.add_field(name="Date", value=row["trade_date"], inline=True)
    embed.add_field(name="Transaction", value=row["transaction"], inline=True)
    embed.add_field(name="Cost / Value", value=row["cost_value"], inline=False)
    try:
        sec_form4_str = row["sec_form4"].strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        sec_form4_str = str(row["sec_form4"])
    embed.add_field(name="SEC Form 4", value=sec_form4_str, inline=False)
    embed.set_footer(text=f"ID : {row['id']}")
    if chart_buffer:
        file = discord.File(chart_buffer, filename="chart.png")
        embed.set_image(url="attachment://chart.png")
        return embed, file
    else:
        return embed, None

# -------------------------------
# Fonction qui rafraîchit la connexion et retourne (conn, cursor)
# -------------------------------
def refresh_db_connection():
    conn = get_db_connection()
    if conn:
        cur = conn.cursor(dictionary=True)
        return conn, cur
    return None, None

# -------------------------------
# Publication automatique des transactions non publiées
# -------------------------------
async def process_transactions():
    # Rafraîchir la connexion à chaque itération
    conn, cur = refresh_db_connection()
    if not conn or not cur:
        print("Impossible de se reconnecter à la base de données.")
        return

    select_query = """
    SELECT id, ticker, owner, relationship, trade_date, transaction, cost_value, sec_form4
    FROM quotes
    WHERE published = 0
    """
    try:
        cur.execute(select_query)
        rows = cur.fetchall()
        print(f"DEBUG: {len(rows)} transactions trouvées avec published = 0")
    except mysql.connector.Error as err:
        print(f"Erreur SQL lors du SELECT: {err}")
        conn.close()
        return

    if not rows:
        print("DEBUG: Aucune transaction à publier.")
        conn.close()
        return

    channel = bot.get_channel(CHANNEL_ID)
    if channel is None:
        print("DEBUG: Salon introuvable. Vérifie CHANNEL_ID.")
        conn.close()
        return

    for row in rows:
        embed, file = create_embed_and_file(row)
        try:
            if file:
                await channel.send(embed=embed, file=file)
            else:
                await channel.send(embed=embed)
            print(f"DEBUG: Publication réussie pour la transaction ID {row['id']}")
        except Exception as e:
            print(f"Erreur lors de l'envoi sur Discord: {e}")
            continue

        update_query = "UPDATE quotes SET published = 1 WHERE id = %s"
        try:
            cur.execute(update_query, (row['id'],))
            conn.commit()
        except mysql.connector.Error as err:
            print(f"Erreur SQL lors du UPDATE: {err}")
        await asyncio.sleep(1)
    conn.close()

# -------------------------------
# Boucle asynchrone pour vérifier les transactions toutes les 60 secondes
# -------------------------------
async def check_transactions_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        await process_transactions()
        await asyncio.sleep(60)

# -------------------------------
# Commande slash /ping
# -------------------------------
@bot.slash_command(name="ping", description="Teste la réactivité du bot.")
async def ping(ctx: discord.ApplicationContext):
    await ctx.respond("Pong!")

# -------------------------------
# Commande slash /reload
# -------------------------------
@bot.slash_command(name="reload", description="Déclenche manuellement la publication des transactions non publiées.")
async def reload_transactions(ctx: discord.ApplicationContext):
    await process_transactions()
    await ctx.respond("Reload effectué, vérifie le salon.")

# -------------------------------
# Commande slash /save
# -------------------------------
@bot.slash_command(name="save", description="Marque une transaction comme sauvegardée pour éviter sa suppression automatique.")
async def save(ctx: discord.ApplicationContext, transaction_id: discord.Option(int, "ID de la transaction à sauvegarder")):
    conn, cur = refresh_db_connection()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD.")
        return
    update_query = "UPDATE quotes SET saved = 1 WHERE id = %s"
    try:
        cur.execute(update_query, (transaction_id,))
        conn.commit()
        if cur.rowcount > 0:
            await ctx.respond(f"La transaction avec l'ID {transaction_id} a été sauvegardée.")
        else:
            await ctx.respond(f"Aucune transaction trouvée avec l'ID {transaction_id}.")
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur lors de la sauvegarde : {err}")
    conn.close()

# -------------------------------
# Commande slash /delete
# -------------------------------
@bot.slash_command(name="delete", description="Supprime une transaction de la base (par ID).")
async def delete(ctx: discord.ApplicationContext, transaction_id: discord.Option(int, "ID de la transaction à supprimer")):
    conn, cur = refresh_db_connection()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD.")
        return
    delete_query = "DELETE FROM quotes WHERE id = %s"
    try:
        cur.execute(delete_query, (transaction_id,))
        conn.commit()
        if cur.rowcount > 0:
            await ctx.respond(f"La transaction avec l'ID {transaction_id} a été supprimée.")
        else:
            await ctx.respond(f"Aucune transaction trouvée avec l'ID {transaction_id}.")
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur lors de la suppression : {err}")
    conn.close()

# -------------------------------
# Commande slash /history
# -------------------------------
@bot.slash_command(name="history", description="Affiche la liste des transactions sauvegardées (saved=1).")
async def history(ctx: discord.ApplicationContext):
    conn, cur = refresh_db_connection()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD.")
        return
    select_query = "SELECT id, ticker FROM quotes WHERE saved = 1 ORDER BY id"
    try:
        cur.execute(select_query)
        rows = cur.fetchall()
        if not rows:
            await ctx.respond("Aucune transaction sauvegardée.")
            conn.close()
            return
        msg = "**Transactions sauvegardées :**\n"
        for row in rows:
            msg += f"ID: {row['id']} | Ticker: {row['ticker']}\n"
        await ctx.respond(msg)
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur SQL lors de la récupération de l'historique : {err}")
    conn.close()

# -------------------------------
# Commande slash /transaction
# -------------------------------
@bot.slash_command(name="transaction", description="Affiche le détail d'une transaction (embed + graphique) par ID.")
async def transaction_cmd(ctx: discord.ApplicationContext, transaction_id: discord.Option(int, "ID de la transaction")):
    conn, cur = refresh_db_connection()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD.")
        return
    select_query = """
    SELECT id, ticker, owner, relationship, trade_date, transaction, cost_value, sec_form4, saved
    FROM quotes
    WHERE id = %s
    """
    try:
        cur.execute(select_query, (transaction_id,))
        row = cur.fetchone()
        if not row:
            await ctx.respond(f"Aucune transaction trouvée avec l'ID {transaction_id}.")
            conn.close()
            return
        embed, file = create_embed_and_file(row)
        if file:
            await ctx.respond(embed=embed, file=file)
        else:
            await ctx.respond(embed=embed)
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur SQL lors de la récupération de la transaction : {err}")
    conn.close()

# -------------------------------
# Boucle automatique au démarrage
# -------------------------------
@bot.event
async def on_ready():
    print(f"Bot connecté : {bot.user} (ID: {bot.user.id})")
    bot.loop.create_task(check_transactions_loop())

# -------------------------------
# Lancement du bot
# -------------------------------
# Charger l'extension
bot.load_extension("ext_finviz_buy")
bot.load_extension("ext_finviz_sell")
bot.load_extension("ext_finviz_bigbuy")
bot.load_extension("ext_finviz_news")

# Lancer le bot
bot.run(TOKEN)

