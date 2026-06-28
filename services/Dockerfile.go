# Generic Go service build. Build with: docker build --build-arg SVC=processor -f services/Dockerfile.go .
FROM golang:1.26 AS build
ARG SVC
WORKDIR /app
COPY services/${SVC} ./
RUN CGO_ENABLED=0 go build -o /out/app .

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
