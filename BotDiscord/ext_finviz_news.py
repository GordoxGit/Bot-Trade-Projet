import discord
import mysql.connector
import asyncio
import io
import random
from datetime import datetime, timedelta
import re
from urllib.parse import urlparse
import textwrap
import aiohttp

# Canal Discord pour les news
NEWS_CHANNEL_ID = 1349776011276849303

# Configuration de la base de données pour les news
db_config_news = {
    'host': 'localhost',
    'user': 'root',
    'password': 'uD4kHXXn2W9gFPb',
    'database': 'BotScrappingNews'
}

# Fonction pour établir une connexion à la BDD NEWS
def get_db_connection_news():
    try:
        conn = mysql.connector.connect(**db_config_news)
        return conn
    except mysql.connector.Error as err:
        print(f"Erreur de connexion à BotScrappingNews: {err}")
        return None

# Fonction qui rafraîchit la connexion et retourne (conn, cursor) pour NEWS
def refresh_db_connection_news():
    conn = get_db_connection_news()
    if conn:
        cur = conn.cursor(dictionary=True)
        return conn, cur
    return None, None

# Fonction pour obtenir une couleur en fonction de la source ou du contenu de la news
def get_news_color(source, title):
    # Couleurs par défaut pour différents types de sources
    source_colors = {
        'CNBC': 0x0078d4,         # Bleu
        'Reuters': 0x005EA8,       # Bleu foncé
        'MarketWatch': 0x83b81a,   # Vert
        'Bloomberg': 0x000000,     # Noir
        'WSJ': 0xda2128,           # Rouge WSJ
        'Barron': 0x0077b6,        # Bleu clair
        'Yahoo': 0x6001d2,         # Violet
        'Seeking Alpha': 0x015ea5, # Bleu
        'Benzinga': 0x6d9eeb,      # Bleu ciel
        'The Motley Fool': 0x00d1c1, # Turquoise
        'Zacks': 0x00843d,         # Vert foncé
    }
    
    # Mots clés pour attribuer des couleurs spécifiques (français et anglais)
    keyword_colors = {
        # Mots clés en anglais
        r'\bearnings\b': 0x4CAF50,
        r'\bbeat.{1,10}estimate': 0x4CAF50,
        r'\bmiss.{1,10}estimate': 0xf44336,
        r'\bupgrade': 0x4CAF50,
        r'\bdowngrade': 0xf44336,
        r'\bbuyout\b': 0x9c27b0,
        r'\bmerger\b': 0x9c27b0,
        r'\bacquisition\b': 0x9c27b0,
        r'\binitiat.{1,10}coverage': 0x2196F3,
        r'\bIPO\b': 0xFFC107,
        r'\blayoffs\b': 0xff9800,
        r'\bsurge': 0x4CAF50,
        r'\bplummet': 0xf44336,
        r'\bslide': 0xf44336,
        r'\bsoar': 0x4CAF50,
        r'\brally': 0x4CAF50,
        r'\btumble': 0xf44336,
        r'\bcrash': 0xf44336,
        r'\brecall': 0xff9800,
        r'\bFDA\b': 0x2196F3,
        r'\bapproval': 0x4CAF50,
        r'\brejection': 0xf44336,
        r'\bcurrency': 0xFFC107,
        r'\bgold\b': 0xFFC107,
        r'\boil\b': 0x795548,
        r'\bcrypto': 0xFFC107,
        r'\bbitcoin': 0xFFA000,
        r'\bethers*um': 0x3F51B5,
        
        # Mots clés en français
        r'\bbénéfices\b': 0x4CAF50,
        r'\bdépasse.{1,10}prévisions': 0x4CAF50,
        r'\brate.{1,10}prévisions': 0xf44336,
        r'\baugmentation': 0x4CAF50,
        r'\bramassage\b': 0x9c27b0,
        r'\bfusion\b': 0x9c27b0,
        r'\bacquisition\b': 0x9c27b0,
        r'\bcouverture': 0x2196F3,
        r'\blicenciements\b': 0xff9800,
        r'\bhausse': 0x4CAF50,
        r'\bchute': 0xf44336,
        r'\bglissement': 0xf44336,
        r'\benvolée': 0x4CAF50,
        r'\brepli': 0xf44336,
        r'\bcrash': 0xf44336,
        r'\brappel': 0xff9800,
        r'\bapprobation': 0x4CAF50,
        r'\brejet': 0xf44336,
        r'\bdevise': 0xFFC107,
        r'\bor\b': 0xFFC107,
        r'\bpétrole\b': 0x795548,
        r'\bcrypto': 0xFFC107,
        r'\bbitcoin': 0xFFA000,
        r'\bethers*um': 0x3F51B5,
    }
    
    # Vérifier d'abord si la source a une couleur spécifique
    for src, color in source_colors.items():
        if src.lower() in source.lower():
            return color
    
    # Sinon, vérifier les mots-clés dans le titre
    title_lower = title.lower()
    for keyword, color in keyword_colors.items():
        if re.search(keyword, title_lower):
            return color
    
    # Valeur par défaut si aucune correspondance
    return 0x607d8b  # Gris bleuté

# Fonction pour extraire le domaine d'une URL
def get_domain(url):
    try:
        domain = urlparse(url).netloc
        # Supprimer www. si présent
        if domain.startswith('www.'):
            domain = domain[4:]
        # Extraire le nom du site principal (ex: bloomberg.com -> bloomberg)
        main_domain = domain.split('.')[0]
        return main_domain.capitalize()
    except:
        return "News"

# Fonction pour formater une date relative en français
def format_relative_time(dt):
    now = datetime.now()
    diff = now - dt
    
    if diff.days > 1:
        return f"Il y a {diff.days} jours"
    elif diff.days == 1:
        return "Hier"
    elif diff.seconds >= 3600:
        hours = diff.seconds // 3600
        return f"Il y a {hours} heure{'s' if hours > 1 else ''}"
    elif diff.seconds >= 60:
        minutes = diff.seconds // 60
        return f"Il y a {minutes} minute{'s' if minutes > 1 else ''}"
    else:
        return "À l'instant"

# Fonction pour obtenir une icône en fonction de la source
def get_source_icon(source):
    icons = {
        'cnbc': 'https://www.cnbc.com/favicon.ico',
        'reuters': 'https://www.reuters.com/pf/resources/images/reuters/favicon.ico',
        'bloomberg': 'https://assets.bwbx.io/s3/javelin/public/javelin/images/favicon-black-63fe5249d3.png',
        'marketwatch': 'https://www.marketwatch.com/favicon.ico',
        'wsj': 'https://www.wsj.com/favicon.ico',
        'yahoo': 'https://s.yimg.com/rz/l/favicon.ico',
        'seekingalpha': 'https://seekingalpha.com/favicon.ico',
        'benzinga': 'https://www.benzinga.com/favicon.ico',
        'fool': 'https://www.fool.com/favicon.ico',
        'zacks': 'https://www.zacks.com/favicon.ico',
    }
    
    source_lower = source.lower()
    for key, url in icons.items():
        if key in source_lower:
            return url
    
    # Icône par défaut
    return "https://finviz.com/favicon.ico"

# Fonction pour récupérer une image d'illustration pour la news (si disponible)
async def get_news_image(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                if response.status != 200:
                    return None
                
                html = await response.text()
                # Rechercher les balises meta pour OpenGraph image ou Twitter image
                og_image_match = re.search(r'<meta[^>]*property=["\']og:image["\'][^>]*content=["\'](.*?)["\']', html)
                twitter_image_match = re.search(r'<meta[^>]*name=["\']twitter:image["\'][^>]*content=["\'](.*?)["\']', html)
                
                image_url = None
                if og_image_match:
                    image_url = og_image_match.group(1)
                elif twitter_image_match:
                    image_url = twitter_image_match.group(1)
                
                return image_url
    except Exception as e:
        print(f"Erreur lors de la récupération de l'image: {e}")
        return None

# Fonction pour créer un embed pour une news (avec traduction française et aperçu)
async def create_news_embed(news):
    # Déterminer la couleur basée sur la source et le titre
    color = get_news_color(news['source'], news.get('title_fr') or news['title'])
    
    # Extraire le domaine de l'URL pour l'affichage
    domain = get_domain(news['link'])
    
    # Formater la date relative
    relative_time = format_relative_time(news['date_time'])
    
    # Utiliser le titre traduit si disponible
    title = news.get('title_fr') if news.get('title_fr') else news['title']
    
    # Utiliser le contenu traduit si disponible
    content = news.get('content_fr') if news.get('content_fr') else news.get('content', "")
    
    # Générer un aperçu intelligent de la news
    if content:
        # Si nous avons du contenu, l'utiliser pour l'aperçu
        preview = textwrap.shorten(content, width=250, placeholder="...")
    else:
        # Sinon, faire un aperçu à partir du titre
        preview = f"Cliquez pour lire l'article complet sur {domain}."
    
    # Créer l'embed avec un style moderne
    embed = discord.Embed(
        title=title,
        url=news['link'],
        description=preview,
        color=color,
        timestamp=news['date_time']
    )
    
    # Ajouter le ticker s'il est disponible
    if news.get('ticker') and news['ticker'].strip():
        embed.add_field(name="Ticker", value=f"${news['ticker']}", inline=True)
    
    # Ajouter la catégorie si disponible (traduite manuellement)
    if news.get('category') and news['category']:
        category_map = {
            'Economy': 'Économie',
            'Markets': 'Marchés',
            'Technology': 'Technologie',
            'Politics': 'Politique',
            'Finance': 'Finance',
            'Business': 'Affaires',
            'Stocks': 'Actions',
            'Commodities': 'Matières premières',
            'Crypto': 'Crypto-monnaies'
        }
        category = category_map.get(news['category'], news['category'])
        embed.add_field(name="Catégorie", value=category, inline=True)
    
    # Configurer le footer avec icône de source
    source_icon = get_source_icon(news['source'])
    embed.set_footer(text=f"{news['source']} • {relative_time}", icon_url=source_icon)
    
    # Ajouter une image si disponible (pour certaines news)
    if random.random() < 0.5:  # 50% de chance d'essayer de récupérer une image
        image_url = await get_news_image(news['link'])
        if image_url:
            embed.set_image(url=image_url)
    
    # Ajouter un auteur (utiliser le domaine comme auteur)
    embed.set_author(name=f"Finviz News • {domain}")
    
    return embed

# Publication automatique des news non publiées avec délai pour éviter de surcharger le VPS
async def process_news(bot):
    # Rafraîchir la connexion à chaque itération
    conn, cur = refresh_db_connection_news()
    if not conn or not cur:
        print("Impossible de se reconnecter à la base de données NEWS.")
        return

    select_query = """
    SELECT id, title, title_fr, link, source, date_time, ticker, category, content, content_fr
    FROM news
    WHERE published = 0
    ORDER BY date_time DESC
    LIMIT 10
    """
    try:
        cur.execute(select_query)
        rows = cur.fetchall()
        print(f"DEBUG NEWS: {len(rows)} news trouvées avec published = 0")
    except mysql.connector.Error as err:
        print(f"Erreur SQL lors du SELECT NEWS: {err}")
        conn.close()
        return

    if not rows:
        print("DEBUG NEWS: Aucune news à publier.")
        conn.close()
        return

    channel = bot.get_channel(NEWS_CHANNEL_ID)
    if channel is None:
        print(f"DEBUG NEWS: Salon NEWS introuvable. ID: {NEWS_CHANNEL_ID}")
        conn.close()
        return

    for row in rows:
        try:
            embed = await create_news_embed(row)
            await channel.send(embed=embed)
            print(f"DEBUG NEWS: Publication réussie pour la news ID {row['id']}")
            
            # Marquer comme publiée
            update_query = "UPDATE news SET published = 1 WHERE id = %s"
            try:
                cur.execute(update_query, (row['id'],))
                conn.commit()
            except mysql.connector.Error as err:
                print(f"Erreur SQL lors de l'UPDATE NEWS: {err}")
            
            # Ajout d'un délai plus long entre chaque message pour éviter de surcharger le VPS
            # et respecter les limites de rate-limit de Discord
            await asyncio.sleep(5)  # 5 secondes entre chaque message
        
        except Exception as e:
            print(f"Erreur lors de l'envoi de la news sur Discord: {e}")
            continue
    
    conn.close()

# Commande slash /news_count
async def news_count(ctx):
    conn, cur = refresh_db_connection_news()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD NEWS.")
        return
    
    count_query = """
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN published = 1 THEN 1 ELSE 0 END) as published,
        SUM(CASE WHEN published = 0 THEN 1 ELSE 0 END) as unpublished
    FROM news
    """
    
    try:
        cur.execute(count_query)
        result = cur.fetchone()
        
        embed = discord.Embed(
            title="📊 Statistiques des News",
            description="Informations sur les news stockées dans la base de données",
            color=0x3498db
        )
        
        embed.add_field(name="Total", value=f"{result['total']} news", inline=True)
        embed.add_field(name="Publiées", value=f"{result['published']} news", inline=True)
        embed.add_field(name="Non publiées", value=f"{result['unpublished']} news", inline=True)
        
        # Récupérer la répartition par source
        source_query = """
        SELECT source, COUNT(*) as count
        FROM news
        GROUP BY source
        ORDER BY count DESC
        LIMIT 5
        """
        
        cur.execute(source_query)
        sources = cur.fetchall()
        
        sources_text = "\n".join([f"• {row['source']}: {row['count']}" for row in sources])
        embed.add_field(name="Top 5 Sources", value=sources_text or "Aucune source", inline=False)
        
        # Récupérer les news les plus récentes
        recent_query = """
        SELECT title_fr, title, date_time
        FROM news
        ORDER BY date_time DESC
        LIMIT 3
        """
        
        cur.execute(recent_query)
        recent = cur.fetchall()
        
        recent_text = "\n".join([f"• {row.get('title_fr') or row['title']} ({format_relative_time(row['date_time'])})" for row in recent])
        embed.add_field(name="News récentes", value=recent_text or "Aucune news récente", inline=False)
        
        embed.set_footer(text="Les news sont conservées pendant 48 heures")
        
        await ctx.respond(embed=embed)
        
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur SQL lors du comptage des news : {err}")
    
    conn.close()

# Commande slash /news_search
async def news_search(ctx, keyword: str):
    conn, cur = refresh_db_connection_news()
    if not conn or not cur:
        await ctx.respond("Erreur de connexion à la BDD NEWS.")
        return
    
    search_query = """
    SELECT id, title, title_fr, link, source, date_time
    FROM news
    WHERE title LIKE %s OR title_fr LIKE %s OR content LIKE %s OR content_fr LIKE %s
    ORDER BY date_time DESC
    LIMIT 5
    """
    
    try:
        search_param = f"%{keyword}%"
        cur.execute(search_query, (search_param, search_param, search_param, search_param))
        results = cur.fetchall()
        
        if not results:
            await ctx.respond(f"Aucune news trouvée contenant '{keyword}'.")
            conn.close()
            return
        
        embed = discord.Embed(
            title=f"🔍 Résultats de recherche pour '{keyword}'",
            description=f"{len(results)} news trouvées",
            color=0x9b59b6
        )
        
        for i, row in enumerate(results):
            relative_time = format_relative_time(row['date_time'])
            # Utiliser le titre traduit si disponible
            title = row.get('title_fr') if row.get('title_fr') else row['title']
            embed.add_field(
                name=f"{i+1}. {title}",
                value=f"[Lire l'article]({row['link']})\n{row['source']} • {relative_time}",
                inline=False
            )
        
        await ctx.respond(embed=embed)
        
    except mysql.connector.Error as err:
        await ctx.respond(f"Erreur SQL lors de la recherche : {err}")
    
    conn.close()

# Commande slash /news_reload
async def news_reload(ctx):
    await ctx.respond("Chargement des news non publiées en cours...")
    await process_news(ctx.bot)
    await ctx.respond("Reload des news terminé.")

# Boucle asynchrone pour vérifier les news toutes les 10 minutes
async def check_news_loop(bot):
    await bot.wait_until_ready()
    while not bot.is_closed():
        await process_news(bot)
        # Augmenter l'intervalle pour réduire la charge sur le VPS
        await asyncio.sleep(600)  # 10 minutes (600 secondes)

# Configuration du Cog pour l'extension
class FinvizNewsCog(discord.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.bot.loop.create_task(check_news_loop(self.bot))
        # Vérifier la connexion initiale
        conn = get_db_connection_news()
        if conn:
            print("Connexion initiale à BotScrappingNews réussie !")
            conn.close()
        else:
            print("ERREUR: Impossible de se connecter à BotScrappingNews")

    # Commande slash /news_count
    @discord.slash_command(name="news_count", description="Affiche des statistiques sur les news stockées")
    async def news_count_cmd(self, ctx):
        await news_count(ctx)
        
    # Commande slash /news_search
    @discord.slash_command(name="news_search", description="Recherche des news par mot-clé")
    async def news_search_cmd(self, ctx, keyword: discord.Option(str, "Mot-clé à rechercher")):
        await news_search(ctx, keyword)
    
    # Commande slash /news_reload
    @discord.slash_command(name="news_reload", description="Force la publication des news en attente")
    async def news_reload_cmd(self, ctx):
        await news_reload(ctx)

def setup(bot):
    bot.add_cog(FinvizNewsCog(bot))
