from DatabaseUtils import DatabaseDeployment
import sys

if __name__ == '__main__':
    sys.argv.append('stable')
    sys.argv.append('dbo.CustOrderHist')
    sys.argv.append('dbo.Test')
    args = []
    args.append('[dbo].[CustOrderHist]')

    args = sys.argv[1:]
    if args[0].strip().lower() == 'stable':
        objects = []
        for o in sys.argv[2:]:
            objects.append(o)
        DatabaseDeployment.DatabaseDeploymentCollections.deploy_to_stable(objects=objects)
