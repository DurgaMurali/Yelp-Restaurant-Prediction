import pandas as pd

def extract_users_for_businesses():
    user_chunks = pd.read_csv('../Dataset/yelp_dataset/yelp_reviews_cleaned.csv', usecols=['user_id'], chunksize=500)
    user_set = set()
    for c in  user_chunks:
        for index, row in c.iterrows():
            user_set.add(row['user_id'])

    print(type(user_set))
    print(len(user_set))

    chunks = pd.read_json('../Dataset/yelp_dataset/yelp_academic_dataset_user.json', orient="records", lines=True, chunksize=500)

    with open('../Dataset/yelp_dataset/yelp_user_cleaned.csv', 'w') as user_write:

        for c in chunks:
            df_to_write = pd.DataFrame()
            for index, row in c.iterrows():
                if(row['user_id'] in user_set):
                    df_to_write = pd.concat([df_to_write, pd.DataFrame(row).transpose()])

            df_to_write.to_csv('../Dataset/yelp_dataset/yelp_user_cleaned.csv', mode='a', encoding='utf-8', index=False)
        
        user_write.close()


extract_users_for_businesses()