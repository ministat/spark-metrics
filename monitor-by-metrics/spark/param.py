class Param(object):
      def __init__(self, args=None):
          if args != None:
             for k, v in args.__dict__.items():
                 self.__dict__[k] = v

      def __getattr__(self, name):
          return self.__dict__[name]

      def __setattr__(self, name, value):
          self.__dict__[name] = value
