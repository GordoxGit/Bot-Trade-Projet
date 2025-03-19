import discord
import mysql.connector
import asyncio
import yfinance as yf
import matplotlib.pyplot as plt
import io
from datetime import datetime
import locale

# Configurer le format des nombres pour l'affichage des montants
try:
    locale.setlocale(locale.LC_ALL, 'fr_FR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    except locale.Error:
        try:
            locale.setlocale(locale.LC_ALL, '')  # Utiliser le locale par défaut du système
        except locale.Error:
            print("Attention: Impossible de configurer le locale, le formatage des nombres pourrait être incorrect")

# Canal Discord pour les transactions BIG BUY - À remplacer par ton ID de canal
BIG_BUY_CHANNEL_ID = 1349811179039621232  # Remplace par l'ID de ton nouveau salon Discord

# Configuration de la base de données pour les transactions BIG BUY
db_config_bigbuy = {
    'host': 'localhost',
    'user': 'root',
    'password': 'uD4kHXXn2W9gFPb',
    'database': 'BotScrappingBigBuy'
}

# Fonction pour établir une connexion à la BDD BIG BUY
def get_db_connection_bigbuy():
    try:
        conn = mysql.connector.connect(**db_config_bigbuy)
        return conn
    except mysql.connector.Error as err:
        print(f"Erreur de connexion à BotScrappingBigBuy: {err}")
        return None

# Fonction pour générer un graphique d'un ticker avec style spécial pour gros achats
def get_stock_chart_bigbuy(ticker: str):
    try:
        data = yf.download(ticker, period="1d", interval="5m")
        if data.empty:
            print(f"Aucune donnée trouvée pour {ticker}")
            return None
        
        # Créer un style de graphique plus élaboré pour les grosses transactions
        plt.figure(figsize=(8, 5))
        plt.plot(data.index, data['Close'], label="Prix de clôture", color="purple", linewidth=2)
        
        # Ajouter volume en barres plus petites
        ax2 = plt.gca().twinx()
        ax2.bar(data.index, data['Volume'], alpha=0.3, color='blue', width=0.001)
        ax2.set_ylabel('Volume', color='blue')
        
        plt.title(f"Graphique pour {ticker.upper()} - TRANSACTION MAJEURE", fontweight='bold', fontsize=14)
        plt.xlabel("Temps")
        plt.ylabel("Prix de clôture ($)")
        plt.grid(True, alpha=0.3)
        plt.legend(loc='upper left')
        
        # Ajouter ombrage de fond
        plt.gca().set_facecolor('#f9f9ff')
        
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=100)
        buf.seek(0)
        plt.close()
        return buf
    except Exception as e:
        print(f"Erreur lors de la génération du graphique pour {ticker}: {e}")
        return None

# Fonction pour formater la valeur de transaction
def format_transaction_value(value):
    try:
        if value >= 1_000_000:
            return f"${value/1_000_000:.2f}M"
        elif value >= 1_000:
            return f"${value/1_000:.2f}K"
        else:
            return f"${value:.2f}"
    except:
        return str(value)

# Fonction pour créer un embed et attacher un graphique pour une transaction BIG BUY
def create_embed_and_file_bigbuy(row):
    chart_buffer = get_stock_chart_bigbuy(row['ticker'])
    
    # Utiliser une couleur distinctive pour les grosses transactions
    embed = discord.Embed(
        title=f"💰 TRANSACTION MAJEURE pour {row['ticker']} 💰",
        description=f"Transaction de grande valeur détectée sur Finviz Insider Trading",
        color=0x9932CC  # Couleur violette pour les gros achats
    )
    
    # Mettre en évidence la valeur de la transaction
    transaction_value = float(row.get('transaction_value', 0))
    formatted_value = format_transaction_value(transaction_value)
    
    embed.add_field(name="Valeur de la transaction", value=f"**{formatted_value}**", inline=False)
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
    embed.set_footer(text=f"ID : {row['id']} | Transaction de grande valeur")
    
    if chart_buffer:
        file = discord.File(chart_buffer, filename="chart_bigbuy.png")
        embed.set_image(url="attachment://chart_bigbuy.png")
        return embed, file
    else:
        return embed, None

# Fonction qui rafraîchit la connexion et retourne (conn, cursor) pour BIG BUY
def refresh_db_connection_bigbuy():
    conn = get_db_connection_bigbuy()
    if conn:
        cur = conn.cursor(dictionary=True)
        return conn, cur
    return None, None

# Publication automatique des transactions BIG BUY non publiées
async def process_bigbuy_transactions(bot):
    # Rafraîchir la connexion à chaque itération
    conn, cur = refresh_db_connection_bigbuy()
    if not conn or not cur:
        print("Impossible de se reconnecter à la base de données BIG BUY.")
        return

    select_query = """
    SELECT id, ticker, owner, relationship, trade_date, transaction, cost_value, sec_form4, transaction_value
    FROM quotes_bigbuy
    WHERE published = 0
    """
    try:
        cur.execute(select_query)
        rows = cur.fetchall()
        print(f"DEBUG BIG BUY: {len(rows)} transactions BIG BUY trouvées avec published = 0")
    except mysql.connector.Error as err:
        print(f"Erreur SQL lors du SELECT BIG BUY: {err}")
        conn.close()
        return

    if not rows:
        print("DEBUG BIG BUY: Aucune transaction BIG BUY à publier.")
        conn.close()
        return

    channel = bot.get_channel(BIG_BUY_CHANNEL_ID)
    if channel is None:
        print(f"DEBUG BIG BUY: Salon BIG BUY introuvable. ID: {BIG_BUY_CHANNEL_ID}")
        conn.close()
        return

    for row in rows:
        embed, file = create_embed_and_file_bigbuy(row)
        try:
            if file:
                await channel.send(content="🔴 **ALERTE TRANSACTION MAJEURE** 🔴", embed=embed, file=file)
            else:
                await channel.send(content="🔴 **ALERTE TRANSACTION MAJEURE** 🔴", embed=embed)
            print(f"DEBUG BIG BUY: Publication réussie pour la transaction BIG BUY ID {row['id']}")
        except Exception as e:
            print(f"Erreur lors de l'envoi de la transaction BIG BUY sur Discord: {e}")
            continue

        update_query = "UPDATE quotes_bigbuy SET published = 1 WHERE id = %s"
        try:
            cur.execute(update_query, (row['id'],))
            conn.commit()
        except mysql.connector.Error as err:
            print(f"Erreur SQL lors de l'UPDATE BIG BUY: {err}")
        await asyncio.sleep(1)
    conn.close()

# Commande slash /save_bigbuy
async def save_bigbuy(ctx, transaction_id: int):
    conn, cur = refresh_db_connection_bigbuy()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD BIG BUY.")
        return
    update_query = "UPDATE quotes_bigbuy SET saved = 1 WHERE id = %s"
    try:
        cur.execute(update_query, (transaction_id,))
        conn.commit()
        if cur.rowcount > 0:
            await ctx.respond(f"La transaction BIG BUY avec l'ID {transaction_id} a été sauvegardée.")
        else:
            await ctx.respond(f"Aucune transaction BIG BUY trouvée avec l'ID {transaction_id}.")
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur lors de la sauvegarde BIG BUY : {err}")
    conn.close()

# Commande slash /delete_bigbuy
async def delete_bigbuy(ctx, transaction_id: int):
    conn, cur = refresh_db_connection_bigbuy()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD BIG BUY.")
        return
    delete_query = "DELETE FROM quotes_bigbuy WHERE id = %s"
    try:
        cur.execute(delete_query, (transaction_id,))
        conn.commit()
        if cur.rowcount > 0:
            await ctx.respond(f"La transaction BIG BUY avec l'ID {transaction_id} a été supprimée.")
        else:
            await ctx.respond(f"Aucune transaction BIG BUY trouvée avec l'ID {transaction_id}.")
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur lors de la suppression BIG BUY : {err}")
    conn.close()

# Commande slash /history_bigbuy
async def history_bigbuy(ctx):
    conn, cur = refresh_db_connection_bigbuy()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD BIG BUY.")
        return
    select_query = """
    SELECT id, ticker, transaction_value 
    FROM quotes_bigbuy 
    WHERE saved = 1 
    ORDER BY transaction_value DESC
    """
    try:
        cur.execute(select_query)
        rows = cur.fetchall()
        if not rows:
            await ctx.respond("Aucune transaction BIG BUY sauvegardée.")
            conn.close()
            return
        msg = "**Transactions BIG BUY sauvegardées :**\n"
        for row in rows:
            formatted_value = format_transaction_value(float(row['transaction_value']))
            msg += f"ID: {row['id']} | Ticker: {row['ticker']} | Valeur: {formatted_value}\n"
        await ctx.respond(msg)
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur SQL lors de la récupération de l'historique BIG BUY : {err}")
    conn.close()

# Commande slash /transaction_bigbuy
async def transaction_bigbuy(ctx, transaction_id: int):
    conn, cur = refresh_db_connection_bigbuy()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD BIG BUY.")
        return
    select_query = """
    SELECT id, ticker, owner, relationship, trade_date, transaction, cost_value, sec_form4, transaction_value, saved
    FROM quotes_bigbuy
    WHERE id = %s
    """
    try:
        cur.execute(select_query, (transaction_id,))
        row = cur.fetchone()
        if not row:
            await ctx.respond(f"Aucune transaction BIG BUY trouvée avec l'ID {transaction_id}.")
            conn.close()
            return
        embed, file = create_embed_and_file_bigbuy(row)
        if file:
            await ctx.respond(embed=embed, file=file)
        else:
            await ctx.respond(embed=embed)
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur SQL lors de la récupération de la transaction BIG BUY : {err}")
    conn.close()

# Boucle asynchrone pour vérifier les transactions BIG BUY toutes les 60 secondes
async def check_bigbuy_transactions_loop(bot):
    await bot.wait_until_ready()
    while not bot.is_closed():
        await process_bigbuy_transactions(bot)
        await asyncio.sleep(60)

# Configuration du Cog pour l'extension
class FinvizBigBuyCog(discord.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.bot.loop.create_task(check_bigbuy_transactions_loop(self.bot))
        # Vérifier la connexion initiale
        conn = get_db_connection_bigbuy()
        if conn:
            print("Connexion initiale à BotScrappingBigBuy réussie !")
            conn.close()
        else:
            print("ERREUR: Impossible de se connecter à BotScrappingBigBuy")

    # Commande slash /save_bigbuy
    @discord.slash_command(name="save_bigbuy", description="Marque une transaction BIG BUY comme sauvegardée")
    async def save_bigbuy_cmd(self, ctx, transaction_id: discord.Option(int, "ID de la transaction BIG BUY à sauvegarder")):
        await save_bigbuy(ctx, transaction_id)

    # Commande slash /delete_bigbuy
    @discord.slash_command(name="delete_bigbuy", description="Supprime une transaction BIG BUY de la base (par ID)")
    async def delete_bigbuy_cmd(self, ctx, transaction_id: discord.Option(int, "ID de la transaction BIG BUY à supprimer")):
        await delete_bigbuy(ctx, transaction_id)

    # Commande slash /history_bigbuy
    @discord.slash_command(name="history_bigbuy", description="Affiche la liste des transactions BIG BUY sauvegardées")
    async def history_bigbuy_cmd(self, ctx):
        await history_bigbuy(ctx)

    # Commande slash /transaction_bigbuy
    @discord.slash_command(name="transaction_bigbuy", description="Affiche le détail d'une transaction BIG BUY (embed + graphique) par ID")
    async def transaction_bigbuy_cmd(self, ctx, transaction_id: discord.Option(int, "ID de la transaction BIG BUY")):
        await transaction_bigbuy(ctx, transaction_id)

    # Commande slash /reload_bigbuy
    @discord.slash_command(name="reload_bigbuy", description="Déclenche manuellement la publication des transactions BIG BUY non publiées")
    async def reload_bigbuy_cmd(self, ctx):
        await process_bigbuy_transactions(self.bot)
        await ctx.respond("Reload BIG BUY effectué, vérifie le salon.")

    # Commande slash /top_bigbuy
    @discord.slash_command(name="top_bigbuy", description="Affiche les plus grosses transactions en BDD")
    async def top_bigbuy_cmd(self, ctx, limit: discord.Option(int, "Nombre de transactions à afficher", default=5)):
        conn, cur = refresh_db_connection_bigbuy()
        if not conn or not cur:
            await ctx.respond("Erreur de connexion à la BDD BIG BUY.")
            return
        
        select_query = """
        SELECT id, ticker, owner, transaction_value, trade_date
        FROM quotes_bigbuy
        ORDER BY transaction_value DESC
        LIMIT %s
        """
        
        try:
            cur.execute(select_query, (limit,))
            rows = cur.fetchall()
            
            if not rows:
                await ctx.respond("Aucune transaction BIG BUY trouvée.")
                conn.close()
                return
                
            embed = discord.Embed(
                title="🏆 TOP DES PLUS GROSSES TRANSACTIONS D'ACHAT 🏆",
                description=f"Les {limit} plus grosses transactions d'achat d'initiés",
                color=0xFFD700  # Couleur or
            )
            
            for i, row in enumerate(rows):
                formatted_value = format_transaction_value(float(row['transaction_value']))
                rank_emoji = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"][i] if i < 10 else f"{i+1}."
                
                embed.add_field(
                    name=f"{rank_emoji} {row['ticker']} - {formatted_value}",
                    value=f"**Acheteur:** {row['owner']}\n**Date:** {row['trade_date']}\n**ID:** {row['id']}",
                    inline=False
                )
                
            await ctx.respond(embed=embed)
            
        except mysql.connector.Error as err:
            await ctx.respond(f"Erreur SQL lors de la récupération du TOP BIG BUY : {err}")
        
        conn.close()

def setup(bot):
    bot.add_cog(FinvizBigBuyCog(bot))
