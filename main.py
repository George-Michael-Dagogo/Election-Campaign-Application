def app():
    import tweepy
    import configparser
    import pandas as pd
    import psycopg2
    from datetime import date
    import datetime 
    import psycopg2
    import pandas as pd
    from sqlalchemy import create_engine
    from prefect import Flow,task
    from prefect.schedules import IntervalSchedule

    #pip install "prefect==1.*"
    today = datetime.date.today()
    week_ago = datetime.date.today() - datetime.timedelta(days=7)
    yester = datetime.date.today() - datetime.timedelta(days=1)

    config = configparser.ConfigParser()
    config.read("config.ini")

    @task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
    def get_data():
        api_key = config['twitter']['api_key']
        api_key_secret = config['twitter']['api_key_secret']
        access_token = config['twitter']['access_token']
        access_token_secret = config['twitter']['access_token_secret']
        auth = tweepy.OAuthHandler(api_key,api_key_secret)
        auth.set_access_token(access_token,access_token_secret)

        api = tweepy.API(auth)

        keywords = ['Buhari OR APC OR  PeterObi OR Tinubu OR PDP OR Atiku OR LabourParty']
        #keywords = ['Buhari','APC', 'PeterObi','Tinubu','Atiku']
        #it seems the api does not return every tweet containing at least one or every keyword, it returns the only tweets that contains every keyword
        #solution was to use the OR in the keywords string as this is for tweets search only and might give errors in pure python
        limit = 10000

        tweets = tweepy.Cursor(api.search_tweets, q = keywords,count = 200, tweet_mode = 'extended',geocode='9.0820,8.6753,450mi', until=today).items(limit)

        columns = ['time_created', 'screen_name','name', 'tweet','loca_tion', 'descrip_tion','verified','followers', 'source','geo_enabled','retweet_count','truncated','lang','likes']
        data = []


        for tweet in tweets:
            data.append([tweet.created_at, tweet.user.screen_name, tweet.user.name,tweet.full_text, tweet.user.location, tweet.user.description,tweet.user.verified,tweet.user.followers_count,tweet.source,tweet.user.geo_enabled,tweet.retweet_count,tweet.truncated,tweet.lang,tweet.favorite_count])
            
        df = pd.DataFrame(data , columns=columns)
        df = df[~df.tweet.str.contains("RT")]
        #removes retweeted tweets
        df = df.reset_index(drop = True)
        print(df.time_created)


        conn_string = config['twitter']['conn_string']
        
        db = create_engine(conn_string)
        conn = db.connect()

        df.to_sql('election', con=conn, if_exists='append',
                index=False)
        conn = psycopg2.connect(database=config['twitter']['name'],
                                    user=config['twitter']['user'], 
                                    password=config['twitter']['password'],
                                    host=config['twitter']['hostname']
            )
        conn.autocommit = True
        cursor = conn.cursor()
        
        sql1 = '''DELETE FROM election WHERE time_created < current_timestamp - interval '10' day;'''
        cursor.execute(sql1)

        sql2 = '''SELECT COUNT(*) FROM election;'''
        cursor.execute(sql2)

        for i in cursor.fetchall():
            print(i)
        
        # conn.commit()
        conn.close()

    def flow_caso(schedule=None):
        """
        this function is for the orchestraction/scheduling of this script
        """
        with Flow("primis",schedule=schedule) as flow:
            Extract_Transform = get_data()
        return flow


    schedule = IntervalSchedule(
        start_date = datetime.datetime.now() + datetime.timedelta(seconds = 2),
        interval = datetime.timedelta(hours=24)
    )
    flow=flow_caso(schedule)

    flow.run()

app()