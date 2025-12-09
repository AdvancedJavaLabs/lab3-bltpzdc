import matplotlib.pyplot as plt

sizes = [100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 700_000, 800_000]
times = [2.427, 3.436, 3.411, 3.413, 3.385, 3.407, 4.435, 4.476]

plt.figure(figsize=(10, 6))
plt.plot(sizes, times, marker='o')

plt.title("Зависимость времени выполнения MapReduce от размера входного файла")
plt.xlabel("Размер файла (строки)")
plt.ylabel("Время выполнения (секунды)")
plt.grid(True)

plt.tight_layout()
plt.show()
