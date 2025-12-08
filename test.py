
extract_data = []
video_id_list = [
    'gchqnwXLxJw', 'atCPtw4dg7g', 't8aM4HuVLrQ',
    'M82VAcabSiA', '7qj3nuF9Dzw', 'Y74b7WlcEpk', 'Z8nEEdXtyYg'
]

def batch_list(video_id_list, batch_size):
    for i in range(0, len(video_id_list), batch_size):
        yield video_id_list[i:i + batch_size]

for batch in batch_list(video_id_list, 3):
           videos_ids_str = "," .join(batch)
           print (batch)
           print(videos_ids_str)





