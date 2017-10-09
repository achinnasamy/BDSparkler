from spark_job import sql_schema_job


import sys,getopt


def help():
    help_statement = "HELP !!!"
    return help_statement



if __name__ == "__main__":

    job = ''
    file = ''

    options, remainder = getopt.getopt(sys.argv[1:], 'o:h', ['job=', 'file=', 'help'])

    for opt, arg in options:

        if opt in ('-j', '--job'):
            job = arg
        elif opt in ('-f', '--file'):
            file = arg
        elif opt in ('-h', '--help'):
            print "\n\n\n HELP \n\n\n"
            print help()
            sys.exit()
        else:
            print "\n\n\n HELP \n\n\n"
            print help()
            sys.exit()

    print "Spark JOB Started ..."
    sql_schema_job.main(file)



