using System;
using System.Collections.Generic;
using EVS.Logging;
using Microsoft.Extensions.DependencyInjection;

namespace Oesebus.Order.Infrastructure.Clients
{
  public static class BundleHelpers
  {
    public static IServiceCollection AddBundle(this IServiceCollection collection, Func<ILogger,IList<string>,IServiceCollection> package)
    {
      var employees = new List<string>() { "Issou", "Oesebus", "Spiderman" };
      var logger = collection.BuildServiceProvider().GetService<ILogger>();
      return package(logger,employees);
    }

  }
}
