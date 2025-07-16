import codecs
import csv
from traceback import print_stack

import redis

# Try to import RediSearch components, fallback if not available
try:
    from redis.commands.search.field import TextField, NumericField, TagField
    from redis.commands.search.indexDefinition import IndexDefinition, IndexType
    from redis.commands.search.query import Query

    REDISEARCH_AVAILABLE = True
    print("RediSearch imports successful!")
except ImportError:
    print("RediSearch not available")
    REDISEARCH_AVAILABLE = False


class RedisClient:
    redis = None

    def __init__(self):
        self.redis = None

    """
    Connect to redis
    """
    def connect(self):
        try:

            self.redis = redis.Redis(
                host='localhost',
                port=6379,
                db=0,
                decode_responses=True
            )

            self.redis.ping()
            print("Connect to Redis successfully!")
            return True
        except Exception as e:
            print(f"Failed to connect to Redis: {e}")
            print_stack()
            return False

    """
    Load the users dataset into Redis DB.
    """
    def load_users(self, file):
        result = 0
        try:
            with codecs.open(file, 'r', 'utf-8') as f:
                content = f.read()

            # Parse the space-delimited data
            lines = content.strip().split('\n')
            pipe = self.redis.pipeline()

            for line in lines:
                # Split by quotes and spaces to parse the format
                parts = []
                current = ""
                in_quotes = False

                i = 0
                while i < len(line):
                    if line[i] == '"':
                        if in_quotes:
                            parts.append(current)
                            current = ""
                            in_quotes = False
                        else:
                            in_quotes = True
                    elif line[i] == ' ' and not in_quotes:
                        if current:
                            parts.append(current)
                            current = ""
                    else:
                        current += line[i]
                    i += 1

                if current:
                    parts.append(current)

                if len(parts) >= 22:  # Check if we are having all required fields
                    user_id = parts[0]
                    user_data = {
                        'first_name': parts[2],
                        'last_name': parts[4],
                        'email': parts[6],
                        'gender': parts[8],
                        'ip_address': parts[10],
                        'country': parts[12],
                        'country_code': parts[14],
                        'city': parts[16],
                        'longitude': parts[18],
                        'latitude': parts[20],
                        'last_login': parts[22] if len(parts) > 22 else parts[21]
                    }

                    # Store as hash
                    pipe.hset(user_id, mapping=user_data)
                    result += 1

            pipe.execute()
            print(f"Load data for user: {result} users loaded")
            return result

        except Exception as e:
            print(f"Error loading users: {e}")
            print_stack()
            return 0

    """
    Load the scores dataset into Redis DB.
    """
    def load_scores(self):
        pipe = self.redis.pipeline()
        result_count = 0

        try:
            # Read the userscores.csv file
            with codecs.open('userscores.csv', 'r', 'utf-8') as file:
                reader = csv.DictReader(file)

                for row in reader:
                    user_id = row['user:id']
                    score = float(row['score'])
                    leaderboard = row['leaderboard']

                    # Add to sorted set for each leaderboard
                    leaderboard_key = f"leaderboard:{leaderboard}"
                    pipe.zadd(leaderboard_key, {user_id: score})
                    result_count += 1

            result = pipe.execute()
            print(f"Load data for scores: {result_count} scores loaded")
            return result

        except Exception as e:
            print(f"Error loading scores: {e}")
            print_stack()
            return []

    """
    Create secondary index for searching (only if RediSearch is available)
    """
    def create_index(self):
        if not REDISEARCH_AVAILABLE:
            print("RediSearch not available, skipping index creation")
            return False

        try:
            # Create index for user documents
            index_def = IndexDefinition(
                index_type=IndexType.HASH,
                prefix=["user:"]
            )

            schema = [
                TextField("first_name"),
                TextField("last_name"),
                TextField("email"),
                TagField("gender"),
                TagField("country"),
                TagField("country_code"),
                TextField("city"),
                NumericField("latitude"),
                NumericField("longitude"),
                NumericField("last_login")
            ]

            self.redis.ft("user_index").create_index(schema, definition=index_def)
            print("Index created successfully!")
            return True

        except Exception as e:
            if "Index already exists" in str(e):
                print("Index already exists, continuing...")
                return True
            else:
                print(f"Error creating index: {e}")
                return False

    """
    Return all the attributes of the user by usr
    """
    def query1(self, usr):
        print("Executing query 1.")
        try:
            user_key = f"user:{usr}"
            result = self.redis.hgetall(user_key)

            if result:
                print(f"User {usr} attributes:")
                for key, value in result.items():
                    print(f"  {key}: {value}")
            else:
                print(f"User {usr} not found")

            return result

        except Exception as e:
            print(f"Error in query1: {e}")
            return None

    """
    Return the coordinate (longitude and latitude) of the user by the usr.
    """
    def query2(self, usr):
        print("Executing query 2.")
        try:
            user_key = f"user:{usr}"
            coordinates = self.redis.hmget(user_key, 'longitude', 'latitude')

            if coordinates[0] and coordinates[1]:
                coord_result = {
                    'longitude': coordinates[0],
                    'latitude': coordinates[1]
                }
                print(f"User {usr} coordinates: {coord_result}")
                return coord_result
            else:
                print(f"Coordinates for user {usr} not found")
                return None

        except Exception as e:
            print(f"Error in query2: {e}")
            return None

    """
    Get the keys and last names of the users whose ids do not start with an odd number.
    """
    def query3(self):
        print("Executing query 3.")
        try:
            userids = []
            result_lastnames = []

            # Use SCAN starting from cursor 1280 with a small count
            cursor = 1280
            count = 10  # Small number of elements per call

            while True:
                cursor, keys = self.redis.scan(cursor=cursor, match="user:*", count=count)

                for key in keys:
                    # Extract user ID number from key (e.g., "user:123" -> "123")
                    user_num = key.split(':')[1]

                    # Check if the first digit is not odd (i.e., it's even: 0,2,4,6,8)
                    if user_num and user_num[0] in ['0', '2', '4', '6', '8']:
                        last_name = self.redis.hget(key, 'last_name')
                        if last_name:
                            userids.append(key)
                            result_lastnames.append(last_name)

                # Break if we've returned to the start (cursor = 0) or processed enough
                if cursor == 0:
                    break

            print(f"Found {len(userids)} users with IDs not starting with odd numbers:")
            for uid, lastname in zip(userids[:10], result_lastnames[:10]):
                # Print first 10
                print(f"  {uid}: {lastname}")

            return userids, result_lastnames

        except Exception as e:
            print(f"Error in query3: {e}")
            return [], []

    """
    Return the females in China or Russia with latitude between 40 and 46.
    """
    def query4(self):
        print("Executing query 4.")
        try:
            if REDISEARCH_AVAILABLE:
                # Use RediSearch if available
                self.create_index()

                # Query: gender=female AND (country=China OR country=Russia) AND latitude between 40 and 46
                query = Query("@gender:{female} ((@country:{China}) | (@country:{Russia})) @latitude:[40 46]")

                try:
                    result = self.redis.ft("user_index").search(query)

                    print(f"Found {result.total} female users in China or Russia with latitude 40-46:")

                    users_info = []
                    for doc in result.docs:
                        user_info = {
                            'id': doc.id,
                            'first_name': getattr(doc, 'first_name', ''),
                            'last_name': getattr(doc, 'last_name', ''),
                            'country': getattr(doc, 'country', ''),
                            'latitude': getattr(doc, 'latitude', ''),
                            'email': getattr(doc, 'email', '')
                        }
                        users_info.append(user_info)
                        print(
                            f"  {doc.id}: {user_info['first_name']} {user_info['last_name']} from {user_info['country']} (lat: {user_info['latitude']})")

                    return users_info

                except Exception as search_error:
                    print(f"RediSearch query failed: {search_error}")
                    print("Falling back to manual search...")
                    # Fall through to manual search

            # Fallback method using SCAN and manual filtering
            print("Using manual search method...")
            users_info = []
            cursor = 0

            while True:
                cursor, keys = self.redis.scan(cursor=cursor, match="user:*", count=100)

                for key in keys:
                    user_data = self.redis.hgetall(key)

                    if (user_data.get('gender') == 'female' and
                            user_data.get('country') in ['China', 'Russia'] and
                            user_data.get('latitude')):

                        try:
                            lat = float(user_data['latitude'])
                            if 40 <= lat <= 46:
                                user_info = {
                                    'id': key,
                                    'first_name': user_data.get('first_name', ''),
                                    'last_name': user_data.get('last_name', ''),
                                    'country': user_data.get('country', ''),
                                    'latitude': user_data.get('latitude', ''),
                                    'email': user_data.get('email', '')
                                }
                                users_info.append(user_info)
                                print(
                                    f"  {key}: {user_info['first_name']} {user_info['last_name']} from {user_info['country']} (lat: {user_info['latitude']})")
                        except ValueError:
                            continue

                if cursor == 0:
                    break

            print(f"Found {len(users_info)} female users in China or Russia with latitude 40-46")
            return users_info

        except Exception as e:
            print(f"Error in query4: {e}")
            return []

    """
    Get the email ids of the top 10 players (in terms of score) in leaderboard:2
    """
    def query5(self):
        print("Executing query 5.")
        try:
            # Get top 10 users from leaderboard:2 (highest scores first)
            top_users = self.redis.zrevrange("leaderboard:2", 0, 9, withscores=True)

            result = []
            print("Top 10 players in leaderboard:2:")

            for user_id, score in top_users:
                email = self.redis.hget(user_id, 'email')
                if email:
                    result.append(email)
                    print(f"  {user_id}: {email} (score: {score})")

            return result

        except Exception as e:
            print(f"Error in query5: {e}")
            return []


if __name__ == "__main__":
    rs = RedisClient()

    if rs.connect():
        print("Redis connection successful!")

        # STEP 1: Load data
        print("\n" + "=" * 50)
        print("LOADING DATA...")
        rs.load_users("users.txt")
        rs.load_scores()

        # Create index for searching (only if RediSearch is available)
        if REDISEARCH_AVAILABLE:
            rs.create_index()

        # STEP 2: Execute queries
        print("\n" + "=" * 50)
        print("EXECUTING QUERIES...")

        print("\n" + "=" * 50)
        rs.query1(1)

        print("\n" + "=" * 50)
        rs.query2(2)

        print("\n" + "=" * 50)
        rs.query3()

        print("\n" + "=" * 50)
        rs.query4()

        print("\n" + "=" * 50)
        rs.query5()
    else:
        print("Failed to connect to Redis.")
