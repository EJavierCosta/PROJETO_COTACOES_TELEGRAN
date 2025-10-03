
def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()
    
def trata_query(query):

    # Substitui qualquer coisa depois de FROM por "df" e adicona ", query_modificada"
    query_modificada = re.sub(r"FROM\s+[\w\.\"]+", "FROM df", self.query, flags=re.IGNORECASE)
    query_modificada = re.sub(r"(SELECT\s+.*?)(FROM)", r"\1,_change_type \2", query_modificada, flags=re.IGNORECASE|re.DOTALL)
    return query_modificada