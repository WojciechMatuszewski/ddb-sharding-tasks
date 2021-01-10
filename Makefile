.PHONY: build

deploy:
	sam build
	sam deploy
