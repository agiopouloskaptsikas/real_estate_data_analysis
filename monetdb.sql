CREATE LOADER load_zillow() LANGUAGE PYTHON {

    import pandas as pd

    path = input("Enter File Path: ")

    zillow_df = pd.read_csv(path, delimiter = ",")

    zillow_dict = {}

    for column in zillow_df.columns:

        zillow_dict[column.replace(" ", "_")] = zillow_df[column].tolist()

    _emit.emit(zillow_dict)

};

CREATE TABLE zillow FROM LOADER load_zillow();

-- Extract number of bedrooms. You will implement a UDF that processes the `facts_and_features` column and extracts the number of bedrooms.

CREATE FUNCTION NumberOfBedrooms(input STRING) RETURNS REAL LANGUAGE PYTHON
{
    import re
    import numpy as np

    def helper(input): -- some inputs might be none, which means zero
        try:
            return float(re.search("([0-9]*)\sbds", input).group(1))
        except:
            return float(0)

    function = np.vectorize(helper)
    
    return function(input)
};

SELECT  NumberOfBedrooms(facts_and_features)
FROM    zillow;

-- Extract number of bathrooms. You will implement a UDF that processes the `facts_and_features` column and extracts the number of bathrooms.

CREATE FUNCTION NumberOfBathrooms(input STRING) RETURNS INTEGER LANGUAGE PYTHON
{
    import re
    import numpy as np

    def helper(input):
        try:
            return float(re.search("([0-9]*)\sbds", input).group(1))
        except:
            return -1

    function = np.vectorize(helper)

    return function(input)
};

SELECT  NumberOfBathrooms(facts_and_features)
FROM    zillow;

-- Extract sqft. You will implement a UDF that processes the `facts_and_features` column and extracts the sqft.

CREATE FUNCTION GrossInternalArea(input STRING) RETURNS INTEGER LANGUAGE PYTHON
{
    import re
    import numpy as np

    def helper(input):
        try:
            return float(re.search("([0-9]*)\ssqft", input).group(1))
        except:
            return -1

    function = np.vectorize(helper)

    return function(input)
};

SELECT  GrossInternalArea(facts_and_features)
FROM    zillow;

-- Extract type. You will implement a UDF that processes the `title` column and returns the type of the listing (e.g. condo, house, apartment).

CREATE FUNCTION Type(input STRING) RETURNS STRING LANGUAGE PYTHON
{
    import re
    import numpy as np

    function = np.vectorize(lambda input: re.search("(.*)\sfor sale", input).group(1))

    return function(input)
};

SELECT  Type(title)
FROM    zillow;

-- Extract offer. You will implement a UDF that processes the `title` column and returns the type of offer. This can be `sale`, `rent`, `sold`, `forclose`.

CREATE FUNCTION Offer(input STRING) RETURNS STRING LANGUAGE PYTHON
{
    import re
    import numpy as np

    def helper(input): -- categories are mutually exclusive
        if bool(re.search("sale", input)):
            return "sale"
        elif bool(re.search("rent", input)):
            return "rent"
        elif bool(re.search("sold", input)):
            return "sold"
        else:
            return "foreclose"

    function = np.vectorize(helper)

    return function(input)
};

SELECT  Offer(title)
FROM    zillow;

-- Filter out listings that are not for sale.

CREATE FUNCTION IsForSale(input STRING) RETURNS BOOLEAN LANGUAGE PYTHON
{
    import re
    import numpy as np

    function = np.vectorize(lambda input: bool(re.search("for sale", input)))

    return function(input)
};

SELECT  *
FROM    zillow
WHERE   IsForSale(title);

-- Extract price. You will implement a UDF that processes the `price` column and extract the price. Prices are stored as strings in the CSV. This UDF parses the string and returns the price as an integer.

CREATE FUNCTION Price(input STRING) RETURNS INTEGER LANGUAGE PYTHON
{
    import re
    import numpy as np

    function = np.vectorize(lambda input: int(re.search("([0-9,]+[0-9]+)", input).group(1).replace(",", "")))

    return function(input)
};

SELECT  Price(price)
FROM    zillow;

-- Filter out listings with more than 10 bedrooms.

SELECT  *
FROM    zillow
WHERE   (NumberOfBedrooms(facts_and_features) <= 10);

-- Filter out listings with price greater than 20000000 and lower than 100000.

SELECT  *
FROM    zillow
WHERE   ((Price(price) < 100000) AND (Price(price) > 20000000));

-- Filter out listings that are not houses.

CREATE FUNCTION IsHouse(input STRING) RETURNS BOOLEAN LANGUAGE PYTHON
{
    import re
    import numpy as np

    function = np.vectorize(lambda input: bool(re.search("[Hh]ouse", input)))

    return function(input)
};

SELECT  *
FROM    zillow
WHERE   IsHouse(title);

-- Calculate average price per sqft for houses for sale grouping them by the number of bedrooms.

SELECT		NumberOfBedrooms(facts_and_features)				AS NBDS,
		AVG(Price(price) / GrossInternalArea(facts_and_features))	AS AVGPSQFT

FROM 		zillow

WHERE 		IsHouse(title)							AND
		IsForSale(title) 						AND
		(GrossInternalArea(facts_and_features) > 0)

GROUP BY	NumberOfBedrooms(facts_and_features)

ORDER BY	NumberOfBedrooms(facts_and_features)				ASC;