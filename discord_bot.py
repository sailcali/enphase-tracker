import discord
from discord.ext import commands

def send_to_discord(error, token, channel_id):
    
    client = commands.Bot(command_prefix="+",intents=discord.Intents.default())

    @client.event
    async def on_ready():
        # print(f'{client.user} has connected to Discord!')
        await client.fetch_channel(channel_id)
        channel = client.get_channel(channel_id)
        await channel.send(f"Solar data not recorded :(\nError: {error}")
        await client.close()
        
    try:
        client.run(token)
    except RuntimeError:
        print("ok")

if __name__ == "__main__":
    send_to_discord()