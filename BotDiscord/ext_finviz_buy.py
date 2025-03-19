import discord
import mysql.connector
import asyncio
import yfinance as yf
import matplotlib.pyplot as plt
import io
from datetime import datetime

# Canal Discord pour les transactions BUY
BUY_CHANNEL_ID = 1349771463204540427

# Configuration de la base de données pour les transactions BUY
db_config_buy = {
    'host': 'localhost',
    'user': 'root',
    'password': 'uD4kHXXn2W9gFPb',
    'database': 'BotScrappingBuy'
}

# Fonction pour établir une connexion à la BDD BUY
def get_db_connection_buy():
    try:
        conn = mysql.connector.connect(**db_config_buy)
        return conn
    except mysql.connector.Error as err:
        print(f"Erreur de connexion à BotScrappingBuy: {err}")
        return None

# Fonction pour générer un graphique d'un ticker
def get_stock_chart_buy(ticker: str):
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

# Fonction pour créer un embed et attacher un graphique pour une transaction BUY
def create_embed_and_file_buy(row):
    chart_buffer = get_stock_chart_buy(row['ticker'])
    embed = discord.Embed(
        title=f"Transaction BUY pour {row['ticker']}",
        description="Nouvelle transaction d'achat détectée sur Finviz Insider Trading",
        color=0x00ff00  # Couleur verte pour les achats
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
        file = discord.File(chart_buffer, filename="chart_buy.png")
        embed.set_image(url="attachment://chart_buy.png")
        return embed, file
    else:
        return embed, None

# Fonction qui rafraîchit la connexion et retourne (conn, cursor) pour BUY
def refresh_db_connection_buy():
    conn = get_db_connection_buy()
    if conn:
        cur = conn.cursor(dictionary=True)
        return conn, cur
    return None, None

# Publication automatique des transactions BUY non publiées
async def process_buy_transactions(bot):
    # Rafraîchir la connexion à chaque itération
    conn, cur = refresh_db_connection_buy()
    if not conn or not cur:
        print("Impossible de se reconnecter à la base de données BUY.")
        return

    select_query = """
    SELECT id, ticker, owner, relationship, trade_date, transaction, cost_value, sec_form4
    FROM quotes_buy
    WHERE published = 0
    """
    try:
        cur.execute(select_query)
        rows = cur.fetchall()
        print(f"DEBUG BUY: {len(rows)} transactions BUY trouvées avec published = 0")
    except mysql.connector.Error as err:
        print(f"Erreur SQL lors du SELECT BUY: {err}")
        conn.close()
        return

    if not rows:
        print("DEBUG BUY: Aucune transaction BUY à publier.")
        conn.close()
        return

    channel = bot.get_channel(BUY_CHANNEL_ID)
    if channel is None:
        print(f"DEBUG BUY: Salon BUY introuvable. ID: {BUY_CHANNEL_ID}")
        conn.close()
        return

    for row in rows:
        embed, file = create_embed_and_file_buy(row)
        try:
            if file:
                await channel.send(embed=embed, file=file)
            else:
                await channel.send(embed=embed)
            print(f"DEBUG BUY: Publication réussie pour la transaction BUY ID {row['id']}")
        except Exception as e:
            print(f"Erreur lors de l'envoi de la transaction BUY sur Discord: {e}")
            continue

        update_query = "UPDATE quotes_buy SET published = 1 WHERE id = %s"
        try:
            cur.execute(update_query, (row['id'],))
            conn.commit()
        except mysql.connector.Error as err:
            print(f"Erreur SQL lors de l'UPDATE BUY: {err}")
        await asyncio.sleep(1)
    conn.close()

# Commande slash /save_buy
async def save_buy(ctx, transaction_id: int):
    conn, cur = refresh_db_connection_buy()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD BUY.")
        return
    update_query = "UPDATE quotes_buy SET saved = 1 WHERE id = %s"
    try:
        cur.execute(update_query, (transaction_id,))
        conn.commit()
        if cur.rowcount > 0:
            await ctx.respond(f"La transaction BUY avec l'ID {transaction_id} a été sauvegardée.")
        else:
            await ctx.respond(f"Aucune transaction BUY trouvée avec l'ID {transaction_id}.")
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur lors de la sauvegarde BUY : {err}")
    conn.close()

# Commande slash /delete_buy
async def delete_buy(ctx, transaction_id: int):
    conn, cur = refresh_db_connection_buy()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD BUY.")
        return
    delete_query = "DELETE FROM quotes_buy WHERE id = %s"
    try:
        cur.execute(delete_query, (transaction_id,))
        conn.commit()
        if cur.rowcount > 0:
            await ctx.respond(f"La transaction BUY avec l'ID {transaction_id} a été supprimée.")
        else:
            await ctx.respond(f"Aucune transaction BUY trouvée avec l'ID {transaction_id}.")
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur lors de la suppression BUY : {err}")
    conn.close()

# Commande slash /history_buy
async def history_buy(ctx):
    conn, cur = refresh_db_connection_buy()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD BUY.")
        return
    select_query = "SELECT id, ticker FROM quotes_buy WHERE saved = 1 ORDER BY id"
    try:
        cur.execute(select_query)
        rows = cur.fetchall()
        if not rows:
            await ctx.respond("Aucune transaction BUY sauvegardée.")
            conn.close()
            return
        msg = "**Transactions BUY sauvegardées :**\n"
        for row in rows:
            msg += f"ID: {row['id']} | Ticker: {row['ticker']}\n"
        await ctx.respond(msg)
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur SQL lors de la récupération de l'historique BUY : {err}")
    conn.close()

# Commande slash /transaction_buy
async def transaction_buy(ctx, transaction_id: int):
    conn, cur = refresh_db_connection_buy()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD BUY.")
        return
    select_query = """
    SELECT id, ticker, owner, relationship, trade_date, transaction, cost_value, sec_form4, saved
    FROM quotes_buy
    WHERE id = %s
    """
    try:
        cur.execute(select_query, (transaction_id,))
        row = cur.fetchone()
        if not row:
            await ctx.respond(f"Aucune transaction BUY trouvée avec l'ID {transaction_id}.")
            conn.close()
            return
        embed, file = create_embed_and_file_buy(row)
        if file:
            await ctx.respond(embed=embed, file=file)
        else:
            await ctx.respond(embed=embed)
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur SQL lors de la récupération de la transaction BUY : {err}")
    conn.close()

# Boucle asynchrone pour vérifier les transactions BUY toutes les 60 secondes
async def check_buy_transactions_loop(bot):
    await bot.wait_until_ready()
    while not bot.is_closed():
        await process_buy_transactions(bot)
        await asyncio.sleep(60)

# Configuration du Cog pour l'extension
class FinvizBuyCog(discord.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.bot.loop.create_task(check_buy_transactions_loop(self.bot))
        # Vérifier la connexion initiale
        conn = get_db_connection_buy()
        if conn:
            print("Connexion initiale à BotScrappingBuy réussie !")
            conn.close()
        else:
            print("ERREUR: Impossible de se connecter à BotScrappingBuy")

    # Commande slash /save_buy
    @discord.slash_command(name="save_buy", description="Marque une transaction BUY comme sauvegardée")
    async def save_buy_cmd(self, ctx, transaction_id: discord.Option(int, "ID de la transaction BUY à sauvegarder")):
        await save_buy(ctx, transaction_id)

    # Commande slash /delete_buy
    @discord.slash_command(name="delete_buy", description="Supprime une transaction BUY de la base (par ID)")
    async def delete_buy_cmd(self, ctx, transaction_id: discord.Option(int, "ID de la transaction BUY à supprimer")):
        await delete_buy(ctx, transaction_id)

    # Commande slash /history_buy
    @discord.slash_command(name="history_buy", description="Affiche la liste des transactions BUY sauvegardées")
    async def history_buy_cmd(self, ctx):
        await history_buy(ctx)

    # Commande slash /transaction_buy
    @discord.slash_command(name="transaction_buy", description="Affiche le détail d'une transaction BUY (embed + graphique) par ID")
    async def transaction_buy_cmd(self, ctx, transaction_id: discord.Option(int, "ID de la transaction BUY")):
        await transaction_buy(ctx, transaction_id)

    # Commande slash /reload_buy
    @discord.slash_command(name="reload_buy", description="Déclenche manuellement la publication des transactions BUY non publiées")
    async def reload_buy_cmd(self, ctx):
        await process_buy_transactions(self.bot)
        await ctx.respond("Reload BUY effectué, vérifie le salon.")

def setup(bot):
    bot.add_cog(FinvizBuyCog(bot))
