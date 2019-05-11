import re

FOLDER_PATH = "../data/"


def clean_phone_field(input_path, output_path):
    with open(input_path, 'r') as source, open(output_path, 'w') as output:  # simultaneous reading and writing

        head = ",".join(source.readline().strip().split(",") + ["phone_cleaned\n"])  # read, modify, and write header
        output.write(head)

        for line in source:  # pass through the dataset once: O(n)
            list_line = line.strip().split(",")
            phone_num = list_line[2].strip()

            if "E" in phone_num:  # scientific notation treatment
                cleaned_num = str(int(float(phone_num)))  # assume trailing zeros are legitimate
            else:
                cleaned_num = re.sub("[^0-9]", "", phone_num)  # preserve only numbers

            number = cleaned_num[-7:]  # last 7 digits are treated as number
            area_code = cleaned_num[-10:-7]  # next 3 digits are treated as area code
            country_code = cleaned_num[:-10]  # the rest is treated as country code

            if len(country_code) > 0:  # full number present
                phone_cleaned = f"+{country_code} ({area_code}) {number[:3]}-{number[3:]}"
            elif len(area_code) > 0:  # country code is absent
                phone_cleaned = f"({area_code}) {number[:3]}-{number[3:]}"
            elif len(number) > 0:  # country and area code are absent
                phone_cleaned = f"{number[:3]}-{number[3:]}"
            else:  # the phone number is absent (NULL)
                phone_cleaned = ""

            list_line.append(phone_cleaned + "\n")
            output.write(",".join(list_line))


clean_phone_field(input_path=FOLDER_PATH + "company_contacts.csv",
                  output_path=FOLDER_PATH + "company_contacts_phone_field_cleaned.csv")
