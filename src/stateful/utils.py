def list_of_instance(arr, type):
    if isinstance(arr, (list, tuple, set)):
        return all([isinstance(v, type) for v in arr])
    return False
