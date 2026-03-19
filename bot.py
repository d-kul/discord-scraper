import discord
import os
import random
from discord.ext import commands
from dotenv import load_dotenv
from scraper import scrape_guild

with open("nuh_uh.txt", "r") as file:
    nuh_uh = file.read().split()

load_dotenv()
TOKEN = os.environ['TOKEN']
ADMIN_ID = os.environ['ADMIN_ID']

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(
        command_prefix=";",
        proxy=os.environ.get('http_proxy', None),
        intents=intents)

@bot.hybrid_command(description="Bazinga!")
async def bazinga(ctx: commands.Context):
    await ctx.send(f"Bazinga! Latency: {bot.latency * 1000:.2f} ms")

@bot.hybrid_command(description="Test the auth system. It's like Pokemon!")
async def nope(ctx: commands.Context):
    await ctx.send(random.choice(nuh_uh), ephemeral=True)

@bot.hybrid_command(description="Sync commands.")
async def sync(ctx: commands.Context):
    if ctx.author.id != ADMIN_ID:
        await ctx.send(random.choice(nuh_uh), ephemeral=True)
        return
    await bot.tree.sync()
    await ctx.send("Commands synced.", ephemeral=True)

@bot.hybrid_command(description="Scrape the server's content.")
async def scrape(ctx: commands.Context):
    if ctx.author.id != ADMIN_ID:
        await ctx.send(random.choice(nuh_uh), ephemeral=True)
        return
    if ctx.guild is None:
        await ctx.send("No available guild.", ephemeral=True)
        return
    await ctx.send("Started data scraping.", ephemeral=True)
    await scrape_guild(ctx.guild)

bot.run(TOKEN)
