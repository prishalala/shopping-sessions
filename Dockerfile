FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
COPY native/kernel.cpp /src/kernel.cpp
RUN g++ -O3 -march=x86-64-v3 -std=c++17 -pthread -static -o /kernel /src/kernel.cpp

FROM scratch
COPY --from=build /kernel /app/kernel
ENV KERNEL_QUIET=1
CMD ["/app/kernel"]
