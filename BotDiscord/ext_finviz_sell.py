import discord
import mysql.connector
import asyncio
import yfinance as yf
import matplotlib.pyplot as plt
import io
from datetime import datetime

# Canal Discord pour les transactions SELL
SELL_CHANNEL_ID = 1349771482376441957

# Configuration de la base de données pour les transactions SELL
db_config_sell = {
    'host': 'localhost',
    'user': 'root',
    'password': 'uD4kHXXn2W9gFPb',
    'database': 'BotScrappingSell'
}

# Fonction pour établir une connexion à la BDD SELL
def get_db_connection_sell():
    try:
        conn = mysql.connector.connect(**db_config_sell)
        return conn
    except mysql.connector.Error as err:
        print(f"Erreur de connexion à BotScrappingSell: {err}")
        return None

# Fonction pour générer un graphique d'un ticker
def get_stock_chart_sell(ticker: str):
    try:
        data = yf.download(ticker, period="1d", interval="5m")
        if data.empty:
            print(f"Aucune donnée trouvée pour {ticker}")
            return None
        plt.figure(figsize=(6, 4))
        plt.plot(data.index, data['Close'], label="Prix de clôture", color="red")  # Rouge pour les ventes
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

# Fonction pour créer un embed et attacher un graphique pour une transaction SELL
def create_embed_and_file_sell(row):
    chart_buffer = get_stock_chart_sell(row['ticker'])
    embed = discord.Embed(
        title=f"Transaction SELL pour {row['ticker']}",
        description="Nouvelle transaction de vente détectée sur Finviz Insider Trading",
        color=0xff0000  # Couleur rouge pour les ventes
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
        file = discord.File(chart_buffer, filename="chart_sell.png")
        embed.set_image(url="attachment://chart_sell.png")
        return embed, file
    else:
        return embed, None

# Fonction qui rafraîchit la connexion et retourne (conn, cursor) pour SELL
def refresh_db_connection_sell():
    conn = get_db_connection_sell()
    if conn:
        cur = conn.cursor(dictionary=True)
        return conn, cur
    return None, None

# Publication automatique des transactions SELL non publiées
async def process_sell_transactions(bot):
    # Rafraîchir la connexion à chaque itération
    conn, cur = refresh_db_connection_sell()
    if not conn or not cur:
        print("Impossible de se reconnecter à la base de données SELL.")
        return

    select_query = """
    SELECT id, ticker, owner, relationship, trade_date, transaction, cost_value, sec_form4
    FROM quotes_sell
    WHERE published = 0
    """
    try:
        cur.execute(select_query)
        rows = cur.fetchall()
        print(f"DEBUG SELL: {len(rows)} transactions SELL trouvées avec published = 0")
    except mysql.connector.Error as err:
        print(f"Erreur SQL lors du SELECT SELL: {err}")
        conn.close()
        return

    if not rows:
        print("DEBUG SELL: Aucune transaction SELL à publier.")
        conn.close()
        return

    channel = bot.get_channel(SELL_CHANNEL_ID)
    if channel is None:
        print(f"DEBUG SELL: Salon SELL introuvable. ID: {SELL_CHANNEL_ID}")
        conn.close()
        return

    for row in rows:
        embed, file = create_embed_and_file_sell(row)
        try:
            if file:
                await channel.send(embed=embed, file=file)
            else:
                await channel.send(embed=embed)
            print(f"DEBUG SELL: Publication réussie pour la transaction SELL ID {row['id']}")
        except Exception as e:
            print(f"Erreur lors de l'envoi de la transaction SELL sur Discord: {e}")
            continue

        update_query = "UPDATE quotes_sell SET published = 1 WHERE id = %s"
        try:
            cur.execute(update_query, (row['id'],))
            conn.commit()
        except mysql.connector.Error as err:
            print(f"Erreur SQL lors de l'UPDATE SELL: {err}")
        await asyncio.sleep(1)
    conn.close()

# Commande slash /save_sell
async def save_sell(ctx, transaction_id: int):
    conn, cur = refresh_db_connection_sell()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD SELL.")
        return
    update_query = "UPDATE quotes_sell SET saved = 1 WHERE id = %s"
    try:
        cur.execute(update_query, (transaction_id,))
        conn.commit()
        if cur.rowcount > 0:
            await ctx.respond(f"La transaction SELL avec l'ID {transaction_id} a été sauvegardée.")
        else:
            await ctx.respond(f"Aucune transaction SELL trouvée avec l'ID {transaction_id}.")
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur lors de la sauvegarde SELL : {err}")
    conn.close()

# Commande slash /delete_sell
async def delete_sell(ctx, transaction_id: int):
    conn, cur = refresh_db_connection_sell()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD SELL.")
        return
    delete_query = "DELETE FROM quotes_sell WHERE id = %s"
    try:
        cur.execute(delete_query, (transaction_id,))
        conn.commit()
        if cur.rowcount > 0:
            await ctx.respond(f"La transaction SELL avec l'ID {transaction_id} a été supprimée.")
        else:
            await ctx.respond(f"Aucune transaction SELL trouvée avec l'ID {transaction_id}.")
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur lors de la suppression SELL : {err}")
    conn.close()

# Commande slash /history_sell
async def history_sell(ctx):
    conn, cur = refresh_db_connection_sell()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD SELL.")
        return
    select_query = "SELECT id, ticker FROM quotes_sell WHERE saved = 1 ORDER BY id"
    try:
        cur.execute(select_query)
        rows = cur.fetchall()
        if not rows:
            await ctx.respond("Aucune transaction SELL sauvegardée.")
            conn.close()
            return
        msg = "**Transactions SELL sauvegardées :**\n"
        for row in rows:
            msg += f"ID: {row['id']} | Ticker: {row['ticker']}\n"
        await ctx.respond(msg)
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur SQL lors de la récupération de l'historique SELL : {err}")
    conn.close()

# Commande slash /transaction_sell
async def transaction_sell(ctx, transaction_id: int):
    conn, cur = refresh_db_connection_sell()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD SELL.")
        return
    select_query = """
    SELECT id, ticker, owner, relationship, trade_date, transaction, cost_value, sec_form4, saved
    FROM quotes_sell
    WHERE id = %s
    """
    try:
        cur.execute(select_query, (transaction_id,))
        row = cur.fetchone()
        if not row:
            await ctx.respond(f"Aucune transaction SELL trouvée avec l'ID {transaction_id}.")
            conn.close()
            return
        embed, file = create_embed_and_file_sell(row)
        if file:
            await ctx.respond(embed=embed, file=file)
        else:
            await ctx.respond(embed=embed)
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur SQL lors de la récupération de la transaction SELL : {err}")
    conn.close()

# Boucle asynchrone pour vérifier les transactions SELL toutes les 60 secondes
async def check_sell_transactions_loop(bot):
    await bot.wait_until_ready()
    while not bot.is_closed():
        await process_sell_transactions(bot)
        await asyncio.sleep(60)

# Configuration du Cog pour l'extension
class FinvizSellCog(discord.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.bot.loop.create_task(check_sell_transactions_loop(self.bot))
        # Vérifier la connexion initiale
        conn = get_db_connection_sell()
        if conn:
            print("Connexion initiale à BotScrappingSell réussie !")
            conn.close()
        else:
            print("ERREUR: Impossible de se connecter à BotScrappingSell")

    # Commande slash /save_sell
    @discord.slash_command(name="save_sell", description="Marque une transaction SELL comme sauvegardée")
    async def save_sell_cmd(self, ctx, transaction_id: discord.Option(int, "ID de la transaction SELL à sauvegarder")):
        await save_sell(ctx, transaction_id)

    # Commande slash /delete_sell
    @discord.slash_command(name="delete_sell", description="Supprime une transaction SELL de la base (par ID)")
    async def delete_sell_cmd(self, ctx, transaction_id: discord.Option(int, "ID de la transaction SELL à supprimer")):
        await delete_sell(ctx, transaction_id)

    # Commande slash /history_sell
    @discord.slash_command(name="history_sell", description="Affiche la liste des transactions SELL sauvegardées")
    async def history_sell_cmd(self, ctx):
        await history_sell(ctx)

    # Commande slash /transaction_sell
    @discord.slash_command(name="transaction_sell", description="Affiche le détail d'une transaction SELL (embed + graphique) par ID")
    async def transaction_sell_cmd(self, ctx, transaction_id: discord.Option(int, "ID de la transaction SELL")):
        await transaction_sell(ctx, transaction_id)

    # Commande slash /reload_sell
    @discord.slash_command(name="reload_sell", description="Déclenche manuellement la publication des transactions SELL non publiées")
    async def reload_sell_cmd(self, ctx):
        await process_sell_transactions(self.bot)
        await ctx.respond("Reload SELL effectué, vérifie le salon.")

def setup(bot):
    bot.add_cog(FinvizSellCog(bot))
