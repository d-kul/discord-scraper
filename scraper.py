import discord
import pandas as pd
import logging
from aiolimiter import AsyncLimiter
from typing import Union

BUFFER_SIZE = 500
REQUESTS_PER_SECOND = 50
MEMBERS_FILENAME = "members.csv"
CHANNELS_FILENAME = "channels.csv"
MESSAGES_FILENAME = "messages.csv"
REACTIONS_FILENAME = "reactions.csv"

logger = logging.getLogger(__name__)

with open("skip_channels.txt", "r") as file:
    skip_channels = set(file.read().split())

async def scrape_guild(guild: discord.Guild):
    logger.info(f"scraping guild {guild} (ID: {guild.id})")
    limiter = AsyncLimiter(REQUESTS_PER_SECOND, 1)
    await scrape_members(guild)
    channels = {"data": []}
    await scrape_messages(guild, channels, limiter)
    await flush_data(channels, CHANNELS_FILENAME)
    logger.info(f"done scraping guild {guild} (ID: {guild.id})")

async def scrape_members(guild: discord.Guild):
    logger.info(f"> scraping members")
    members = {
      "data": [],
    }
    for member in guild.members:
        if member.bot:
            continue
        members["data"].append({
            "id": member.id,
            "name": member.display_name,
            "avatar": str(member.avatar),
            "created_at": member.created_at,
            "joined_at": member.joined_at,
            "roles": len(member.roles),
        })
    await flush_data(members, MEMBERS_FILENAME)
    logger.info(f"> done scraping members")

async def scrape_messages(guild: discord.Guild, channels, limiter: AsyncLimiter):
    logger.info(f"> scraping messages")
    messages = {
      "data": [],
      "dtype": {
        "thread": "Int64",
        "reference": "Int64",
      },
    }
    reactions = {"data": []}
    for channel in guild.text_channels:
        channels["data"].append({
            "id": channel.id, 
            "name": channel.name,
            "topic": channel.topic if channel.topic is not None else "",
            "nsfw": channel.nsfw
        })
        await scrape_channel(channel, messages, reactions, limiter);
        await flush_data(messages, MESSAGES_FILENAME)
        await flush_data(reactions, REACTIONS_FILENAME)
    logger.info(f"> done scraping messages")

async def scrape_channel(channel: discord.TextChannel, messages, reactions, limiter: AsyncLimiter):
    if channel.name in skip_channels:
        logger.info(f"> > skipping #{channel} (ID: {channel.id})")
        return
    logger.info(f"> > scraping #{channel} (ID: {channel.id})")
    count = 0
    async for message in channel.history(limit=None):
        count += 1
        await scrape_message(message, messages, reactions, limiter)
        if count % 100 == 0:
            await limiter.acquire()
    for thread in channel.threads:
        await scrape_thread(thread, messages, reactions, limiter)
    logger.info(f"> > done scraping #{channel} (ID: {channel.id})")

async def scrape_thread(thread: discord.Thread, messages, reactions, limiter):
    count = 0
    async for message in thread.history(limit=None):
        count += 1
        await scrape_message(message, messages, reactions, limiter)
        if count % 100 == 0:
            await limiter.acquire()

async def scrape_message(message: discord.Message, messages, reactions, limiter: AsyncLimiter):
    if not message.content or message.author.bot:
        return
    messages["data"].append({
        "id": message.id,
        "channel": message.channel.id,
        "thread": str(message.thread.id) if message.thread is not None else None,
        "reference": str(message.reference.message_id) 
            if message.reference is not None and 
               message.reference.message_id is not None 
            else None,
        "created_at": message.created_at,
        "edited_at": message.edited_at,
        "author": message.author.id,
        "content": message.content,
        "attachments": len(message.attachments),
        "reactions": sum(r.count for r in message.reactions),
        "mentions": len(message.mentions),
    })
    if len(messages["data"]) >= BUFFER_SIZE:
        await flush_data(messages, MESSAGES_FILENAME)

    for reaction in message.reactions:
        count = 0
        async for user in reaction.users():
            count += 1
            await scrape_reaction(message, user, reaction,reactions)
            if count % 100 == 0:
                await limiter.acquire()

async def scrape_reaction(message: discord.Message, user: Union[discord.Member, discord.User], reaction: discord.Reaction, reactions):
    if user.bot:
        return
    reactions["data"].append({
        "user": user.id,
        "message": message.id,
        "reaction": reaction.emoji,
    })
    if len(reactions["data"]) >= BUFFER_SIZE:
        await flush_data(reactions, REACTIONS_FILENAME)

async def flush_data(data, filename):
    if len(data["data"]) == 0:
        return
    header = data.get("header", True)
    df = pd.DataFrame(data["data"]).astype(data.get("dtype", {}))
    logger.debug(f"flushing to {filename}")
    df.to_csv(filename, mode='w' if header else 'a', index=False, header=header)
    data["data"] = []
    data["header"] = False
