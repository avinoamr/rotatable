# Changelog

Changes include:

- Versions With major new features

## 2018-02-13 - Version 3.0.0

### Distinct changes

- Omitting the use of the internal compress & upload queues
- Not reading from stream buffer until rotation is finished
    (piping works as expected of a writable stream)
- Using promises instead of events to manage flow