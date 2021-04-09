class Test:
    def __call__(self):
        self.main()
    
    def main(self):
        print('test')


Test()()