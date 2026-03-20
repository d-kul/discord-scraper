import asyncio
import discord
import pandas as pd
from pathlib import Path
import logging
from aiolimiter import AsyncLimiter
from typing import Union
import json

BUFFER_SIZE = 500
REQUESTS_PER_PERIOD = 1
PERIOD_SECONDS = 1
ERROR_RESTART_SECONDS = 10

MEMBERS_FILENAME = "members.csv"
CHANNELS_FILENAME = "channels.csv"
MESSAGES_FILENAME = "messages.csv"
REACTIONS_FILENAME = "reactions.csv"
CHECKPOINT_FILENAME = "checkpoint.json"

def get_checkpoint(channel, thread=None):
    if channel not in checkpoint:
        return None
    if thread is None:
        return discord.Object(id=checkpoint[channel]["before"])
    if thread in checkpoint[channel]["threads"]:
        return discord.Object(id=checkpoint[channel]["threads"][thread])
    return None

def save_checkpoint(before, channel, thread=None):
    global checkpoint
    if channel not in checkpoint:
        checkpoint[channel] = {"threads": {}}
    if thread is not None:
        checkpoint[channel]["threads"][thread] = before
    else:
        checkpoint[channel]["before"] = before
    with open(CHECKPOINT_FILENAME, 'w') as file:
        json.dump(checkpoint, file, indent=4, default=str)

if Path(CHECKPOINT_FILENAME).is_file():
    with open(CHECKPOINT_FILENAME, 'r') as file:
        checkpoint = json.load(file)
else:
    checkpoint = {}

logger = logging.getLogger(__name__)

with open("skip_channels.txt", "r") as file:
    skip_channels = set(file.read().split())

async def scrape_guild(guild: discord.Guild):
    logger.info(f"scraping guild {guild} (ID: {guild.id})")
    limiter = AsyncLimiter(REQUESTS_PER_PERIOD, PERIOD_SECONDS)
    await scrape_members(guild)
    await scrape_channel_data(guild)
    await scrape_messages(guild, limiter)
    logger.info(f"done scraping guild {guild} (ID: {guild.id})")

async def scrape_members(guild: discord.Guild):
    logger.info(f"> scraping members")
    members = {"data": []}
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

async def scrape_channel_data(guild: discord.Guild):
    logger.info(f"> scraping channel data")
    channels = {
        "data": [],
        "dtype": {
            "parent": "Int64",
        },
    }
    for channel in guild.text_channels:
        channels["data"].append({
            "id": channel.id, 
            "name": channel.name,
            "topic": channel.topic if channel.topic is not None else "",
            "nsfw": channel.nsfw
        })
        for thread in channel.threads:
            channels["data"].append({
                "id": thread.id, 
                "parent": str(channel.id), 
                "name": thread.name,
                "nsfw": channel.nsfw
            })
        async for thread in channel.archived_threads(limit=None):
            channels["data"].append({
                "id": thread.id, 
                "parent": str(channel.id), 
                "name": thread.name,
                "nsfw": channel.nsfw
            })
    await flush_data(channels, CHANNELS_FILENAME)
    logger.info(f"> done scraping channel data")

async def scrape_messages(guild: discord.Guild, limiter: AsyncLimiter):
    while True:
        try:
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
                await scrape_channel(channel, messages, reactions, limiter);
            logger.info(f"> done scraping messages")
        except discord.DiscordServerError as e:
            logger.error(e)
            logger.info(f">restarting in {ERROR_RESTART_SECONDS} s")
            await asyncio.sleep(ERROR_RESTART_SECONDS)
            continue
        break

async def scrape_channel(channel: discord.TextChannel, messages, reactions, limiter: AsyncLimiter):
    if channel.name in skip_channels:
        logger.info(f"> > skipping #{channel} (ID: {channel.id})")
        return
    logger.info(f"> > scraping #{channel} (ID: {channel.id})")

    count = 0
    async for message in channel.history(limit=None, before=get_checkpoint(channel.name)):
        count += 1
        await scrape_message(message, messages, reactions, limiter)
        if count % 100 == 0:
            await limiter.acquire()
    await flush_data(messages, MESSAGES_FILENAME, channel)
    await flush_data(reactions, REACTIONS_FILENAME)

    for thread in channel.threads:
        await scrape_thread(thread, messages, reactions, limiter)

    async for thread in channel.archived_threads(limit=None):
        await scrape_thread(thread, messages, reactions, limiter)

    logger.info(f"> > done scraping #{channel} (ID: {channel.id})")

async def scrape_thread(thread: discord.Thread, messages, reactions, limiter):
    logger.info(f"> > > scraping thread {thread} (ID: {thread.id})")
    count = 0
    async for message in thread.history(limit=None, before=get_checkpoint(thread.parent.name, thread.name)):
        count += 1
        await scrape_message(message, messages, reactions, limiter)
        if count % 100 == 0:
            await limiter.acquire()
    await flush_data(messages, MESSAGES_FILENAME, thread.parent, thread)
    await flush_data(reactions, REACTIONS_FILENAME)
    logger.info(f"> > > done scraping thread {thread} (ID: {thread.id})")

async def scrape_message(message: discord.Message, messages, reactions, limiter: AsyncLimiter):
    if not message.content or message.author.bot or not (
            isinstance(message.channel, discord.TextChannel) or
            isinstance(message.channel, discord.Thread)):
        return
    if isinstance(message.channel, discord.Thread):
        channel_id = message.channel.parent_id
        thread_id = str(message.channel.id)
    else:
        channel_id = message.channel.id
        thread_id = None
    messages["data"].append({
        "id": message.id,
        "channel": channel_id,
        "thread": thread_id,
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

    for reaction in message.reactions:
        count = 0
        async for user in reaction.users():
            count += 1
            await scrape_reaction(message, user, reaction, reactions)
            if count % 100 == 0:
                await limiter.acquire()

    if len(messages["data"]) >= BUFFER_SIZE:
        await flush_data(reactions, REACTIONS_FILENAME)
        await flush_data(messages, MESSAGES_FILENAME, message.channel, message.thread)

async def scrape_reaction(message: discord.Message, user: Union[discord.Member, discord.User], reaction: discord.Reaction, reactions):
    if user.bot:
        return
    reactions["data"].append({
        "user": user.id,
        "message": message.id,
        "reaction": reaction.emoji,
    })

async def flush_data(data, filename, channel=None, thread=None):
    if len(data["data"]) == 0:
        return
    header = data.get("header", True) and (channel is None or len(checkpoint) == 0)
    df = pd.DataFrame(data["data"]).astype(data.get("dtype", {}))
    logger.debug(f"flushing to {filename}")
    df.to_csv(filename, mode='w' if header else 'a', index=False, header=header)
    if channel is not None:
        save_checkpoint(data["data"][-1]["id"], channel.name, thread.name if thread is not None else None)
    data["data"] = []
    data["header"] = False
