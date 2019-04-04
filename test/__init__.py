EXAMPLE_ROWS = (
    ('Transaction_date', 'Product', 'Price', 'Payment_Type', 'Name'),
    ('1/2/09 6:17', 'Product1', '1200', 'Mastercard', 'carolina'),
    ('1/2/09 4:53', 'Product1', '1200', 'Visa', 'Betina'),
    ('1/2/09 13:08', 'Product1', '1200', 'Mastercard', 'Federica e Andrea'),
    ('1/3/09 14:44', 'Product1', '1200', 'Visa', 'Gouya'),
    ('1/4/09 12:56', 'Product2', '3600', 'Visa', 'Gerd W '),
    ('1/4/09 13:19', 'Product1', '1200', 'Visa', 'LAURENCE'),
    ('1/4/09 20:11', 'Product1', '1200', 'Mastercard', 'Fleur'),
    ('1/2/09 20:09', 'Product1', '1200', 'Mastercard', 'adam'),
    ('1/4/09 13:17', 'Product1', '1200', 'Mastercard', 'Renee Elisabeth'),
)


def ex_csv_stream():
    yield 'Transaction_date,Product,Price,Payment_Type,Name\r\n'
    yield '1/2/09 6:17,Product1,1200,Mastercard,carolina\r\n'
    yield '1/2/09 4:53,Product1,1200,Visa,Betina\r\n'
    yield '1/2/09 13:08,Product1,1200,Mastercard,Federica e Andrea\r\n'
    yield '1/3/09 14:44,Product1,1200,Visa,Gouya\r\n'
    yield '1/4/09 12:56,Product2,3600,Visa,Gerd W \r\n'
    yield '1/4/09 13:19,Product1,1200,Visa,LAURENCE\r\n'
    yield '1/4/09 20:11,Product1,1200,Mastercard,Fleur\r\n'
    yield '1/2/09 20:09,Product1,1200,Mastercard,adam\r\n'
    yield '1/4/09 13:17,Product1,1200,Mastercard,Renee Elisabeth\r\n'


def ex_csv_bytestream():
    for csvline in ex_csv_stream():
        yield csvline.encode('utf-8')
