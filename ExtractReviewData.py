import pandas as pd

def extract_reviews_for_businesses():
    business_df = pd.read_csv('../Dataset/yelp_dataset/yelp_business_cleaned.csv', usecols=['Business_ID'])
    business_ids = pd.Series(list(business_df['Business_ID']))

    chunks = pd.read_json('../Dataset/yelp_dataset/yelp_academic_dataset_review.json', orient="records", lines=True, chunksize=500)

    with open('../Dataset/yelp_dataset/yelp_reviews_cleaned.csv', 'w') as rv_write:

        for c in chunks:
            df_to_write = pd.DataFrame()
            for index, row in c.iterrows():
                if(row['business_id'] in business_ids.values):
                    df_to_write = pd.concat([df_to_write, pd.DataFrame(row).transpose()])
                    print(row)
                    print(pd.DataFrame(row).transpose())

            df_to_write.to_csv('../Dataset/yelp_dataset/yelp_reviews_cleaned.csv', mode='a', encoding='utf-8', index=False)
        
        rv_write.close()


extract_reviews_for_businesses()