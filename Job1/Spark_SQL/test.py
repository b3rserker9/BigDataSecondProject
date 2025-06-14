import matplotlib.pyplot as plt

# Dimensioni dei file (in GB)
file_sizes = [0.5, 1, 2, 3, 6.5]

# Tempi di esecuzione stimati in secondi (lineari basati sul log 6.5GB)
execution_times = [90, 190, 410, 640, 1200]  # Esempio di tempi di esecuzione

# Etichette per l’asse X
labels = ['0.5GB', '1GB', '2GB', '3GB', '6.5GB']

plt.figure(figsize=(8,5))
plt.plot(file_sizes, execution_times, marker='o', linestyle='-', color='b')
plt.xticks(file_sizes, labels)
plt.xlabel('Dimensione file')
plt.ylabel('Tempo di esecuzione (s)')
plt.title('Comparazione tempi di esecuzione Hadoop MapReduce per dimensione file')
plt.grid(True)
plt.show()
