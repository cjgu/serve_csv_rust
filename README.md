# serve_csv

This project is built as an experiment for serving item to item recommendations that has been precalculated into a CSV file.


It works by indexing the first column (a integer item_id) then serves up a HTTP server serving the recommendations (rest of the columns in format item_id:score where score is a float).


# Usage

Given a csv

```csv
5,6:0.5,1:0.1
6,5:0.5,1:0.1
1,5:0.1,6:0.1
```

Querying the server using HTTPie gives

```
$ http http://localhost:8000/1
HTTP/1.1 200 OK
Content-Length: 86
Content-Type: application/json
Date: Sat, 01 Apr 2017 09:51:36 GMT
Server: rocket

{
    "recommendations": [
        {
            "item_id": 5,
            "score": 0.1
        },
        {
            "item_id": 6,
            "score": 0.1
        }
    ],
    "status": 200
}
```
