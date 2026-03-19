import discord
import logging
import os
import random
from discord.ext import commands
from dotenv import load_dotenv
from scraper import scrape_guild

with open("nuh_uh.txt", "r") as file:
    nuh_uh = file.read().split()

load_dotenv()
TOKEN = os.environ['TOKEN']
ADMIN_ID = int(os.environ['ADMIN_ID'])
LOG_LEVEL = logging.getLevelName(os.environ.get('LOG_LEVEL', 'INFO'))

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(
        command_prefix=";",
        proxy=os.environ.get('http_proxy', None),
        intents=intents)

logger = logging.getLogger('bot')
logger.setLevel(LOG_LEVEL)
logging.getLogger('scraper').setLevel(LOG_LEVEL)

@bot.hybrid_command(description="Bazinga!")
async def bazinga(ctx: commands.Context):
    logger.info(f"/bazinga from {ctx.author} in #{ctx.channel}, {ctx.guild}")
    await ctx.send(f"Bazinga! Latency: {bot.latency * 1000:.2f} ms")

@bot.hybrid_command(description="Test the auth system. It's like Pokemon!")
async def nope(ctx: commands.Context):
    logger.info(f"/nope from {ctx.author} in #{ctx.channel}, {ctx.guild}")
    await ctx.send(random.choice(nuh_uh), ephemeral=True)

@bot.hybrid_command(description="Sync commands.")
async def sync(ctx: commands.Context):
    logger.info(f"/sync from {ctx.author} in #{ctx.channel}, {ctx.guild}")
    if ctx.author.id != ADMIN_ID:
        await ctx.send(random.choice(nuh_uh), ephemeral=True)
        return
    await bot.tree.sync()
    await ctx.send("Commands synced.", ephemeral=True)

@bot.hybrid_command(description="Scrape the server's content.")
async def scrape(ctx: commands.Context):
    logger.info(f"/scrape from {ctx.author} in #{ctx.channel}, {ctx.guild}")
    if ctx.author.id != ADMIN_ID:
        await ctx.send(random.choice(nuh_uh), ephemeral=True)
        return
    if ctx.guild is None:
        await ctx.send("No available guild.", ephemeral=True)
        return
    await ctx.send("Started data scraping.", ephemeral=True)
    await scrape_guild(ctx.guild)

bot.run(TOKEN, root_logger=True)
