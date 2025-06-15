input_file = 'used_cars_data_clean.csv'
output_name_template = 'used_cars_data_{}.csv'

# dimensioni target in byte: 1GB, 2GB, 3GB, 4GB
target_sizes = [
    int(500 * 1024 ** 2),  # 500 MB
    int(1 * 1024 ** 3),  # 1 GB
    int(2 * 1024 ** 3),  # 2 GB
    int(3 * 1024 ** 3)  # 3 GB
]

with open(input_file, 'r', encoding='utf-8') as infile:
    header = infile.readline()  # leggi header

    file_count = 0
    current_size = 0
    target_index = 0
    outfile = open(output_name_template.format(file_count), 'w', encoding='utf-8')
    outfile.write(header)
    current_size += len(header.encode('utf-8'))

    for line in infile:
        line_size = len(line.encode('utf-8'))

        if current_size + line_size > target_sizes[target_index]:
            outfile.close()
            file_count += 1
            # passa al prossimo target size (se superiamo l'ultimo rimaniamo su quello)
            target_index = min(target_index + 1, len(target_sizes) - 1)
            outfile = open(output_name_template.format(file_count), 'w', encoding='utf-8')
            outfile.write(header)
            current_size = len(header.encode('utf-8'))

        outfile.write(line)
        current_size += line_size

    outfile.close()
