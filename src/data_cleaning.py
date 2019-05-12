import csv
import re
import psycopg2
import yaml


def clean_phone_field(input_path, output_path):

    with open(input_path, 'r') as source, open(output_path, 'w') as output:  # simultaneous reading and writing

        head = source.readline().strip().split(",") + ["phone_cleaned"]  # read, modify, and write header
        csv_writer = csv.writer(output,
                                delimiter=',',
                                quotechar='"',
                                lineterminator='\n')
        csv_writer.writerow(head)

        for line in csv.reader(source,
                               quotechar='"',
                               delimiter=','):  # pass through the dataset once: O(n)
            phone_num = line[2].strip()

            if "E" in phone_num:  # scientific notation treatment
                cleaned_num = str(int(float(phone_num)))  # assume trailing zeros are legitimate
            else:
                cleaned_num = re.sub("[^0-9]", "", phone_num)  # preserve only numbers

            number = cleaned_num[-7:]  # last 7 digits are treated as number
            area_code = cleaned_num[-10:-7]  # next 3 digits are treated as area code
            country_code = cleaned_num[:-10]  # the rest is treated as country code
            if len(country_code) > 3:  # country code is up to 3 digits
                country_code = country_code[-3:]

            if len(country_code) > 0:  # full number present
                phone_cleaned = f"+{country_code} ({area_code}) {number[:3]}-{number[3:]}"
            elif len(area_code) > 0:  # country code is absent
                phone_cleaned = f"({area_code}) {number[:3]}-{number[3:]}"
            elif len(number) > 0:  # country and area code are absent
                phone_cleaned = f"{number[:3]}-{number[3:]}"
            else:  # the phone number is absent (NULL)
                phone_cleaned = ""

            line.append(phone_cleaned)
            csv_writer.writerow(line)


def upload_and_join(local_path, table_name="solution_Alex"):

    with open('config.yml', 'rb') as yaml_file:
        conf = yaml.safe_load(yaml_file)  # load the config file
    print(type(conf))
    conn = psycopg2.connect(**conf)  # connection parameters are saved on a local machine and put into .gitignore
    cur = conn.cursor()

    with open(local_path, 'r') as f:
        head = f.readline().strip().split(",")
        temp_table = "phone_cleaned"
        cur.execute(f"""
                    CREATE TABLE {temp_table} (
                    {head[0]} VARCHAR(256),
                    {head[1]} VARCHAR(128),
                    {head[2]} VARCHAR(128),
                    {head[3]} VARCHAR(256),
                    {head[4]} VARCHAR(32));
                    """)

        cur.copy_expert(f"""
                        COPY {temp_table} FROM STDIN WITH CSV DELIMITER ',' QUOTE '"';
                        """,
                        f)

        cur.execute(f"""
                    ALTER TABLE {temp_table} ADD COLUMN domain VARCHAR(128);

                    UPDATE {temp_table}
                    SET domain = SUBSTRING(homepage_url, '(?:https?:)?(?:\/\/)?(?:[^@\n]+@)?(?:www\.)?([^:\/\n]+)');
                    """)  # extract domain from homepage_url to speed up join in the next query

        conn.commit()

        cur.execute(f"""
                    CREATE TABLE {table_name} AS (
                    SELECT c.name, c.domain, t1.homepage_url, t1.email, t1.phone_cleaned, t1.phone, t1.uuid
                    FROM {temp_table} AS t1, companies as c
                    WHERE t1.domain = c.domain
                    );
                    """)  # Inner join implemented, could be modified to a full outer join (or left/right join)

        cur.execute(f"""
                    DROP TABLE IF EXISTS {temp_table};
                    """)

        conn.commit()
    conn.close()


if __name__ == "__main__":
    FOLDER_PATH = "../data/"
    OUTPUT_FILE = "company_contacts_phone_field_cleaned.csv"

    clean_phone_field(input_path=FOLDER_PATH + "company_contacts.csv",
                      output_path=FOLDER_PATH + OUTPUT_FILE)

    upload_and_join(local_path=FOLDER_PATH + OUTPUT_FILE,
                    table_name="solution_Alex")
