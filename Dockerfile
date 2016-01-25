FROM scratch


COPY glow /

EXPOSE 8930
VOLUME ["/data"]

ENTRYPOINT ["/glow"]
CMD [""]
