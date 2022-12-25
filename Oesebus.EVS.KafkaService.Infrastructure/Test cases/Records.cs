using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace Oesebus.Order.Infrastructure.Test_cases
{
  internal class Records
  {
    private Func<int, int, bool> predicate1;
    private Func<int, int, bool> predicate2;
    private Func<int, int, bool> predicate3;
    private Action<int, int, bool> action1;
    public abstract record TestRecord(string pos, string pos2, int quantity);
    public record TestRecord2(string pos, string pos2, int quantity, int age) : TestRecord(pos, pos2, quantity);

    public void test()
    {
      var t = new TestRecord2("dsdsxd", "sddsd", 25, 39);
      var t2 = new TestRecord2("dsds", "sdsd", 255, 45);
    }

    private async Task GetInfo()
    {
      var response = await new HttpClient().GetAsync("https://medium.com");

      test1((a, b, c) => Console.Write(""));

      var res = predicate1.Invoke(5, 10);

      var res2 = Enumerable.Range(0, 5).Select(x => x * 2);
    }

    public void test1(Action<int, int, bool> actionTest)
    {

    }

  }
}

