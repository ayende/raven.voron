// -----------------------------------------------------------------------
//  <copyright file="Foo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Xunit;

namespace Voron.Tests.Mvcc
{
	public class Foo : StorageTest
	{
		[Fact]
		public void Foo2()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.Root.Add(tx, new string('9', 1300), new MemoryStream(new byte[3]));
				RenderAndShow(tx, Env.Root, 1);
				Env.Root.Add(tx, new string('1', 1300), new MemoryStream(new byte[3]));
				RenderAndShow(tx, Env.Root, 1);
				Env.Root.Add(tx, new string('4', 1000), new MemoryStream(new byte[2]));
				RenderAndShow(tx, Env.Root, 1);
				Env.Root.Add(tx, new string('5', 1300), new MemoryStream(new byte[2]));
				RenderAndShow(tx, Env.Root, 1);
				Env.Root.Add(tx, new string('8', 1300), new MemoryStream(new byte[3]));
				RenderAndShow(tx, Env.Root, 1);
				Env.Root.Add(tx, new string('2', 1300), new MemoryStream(new byte[2]));
				RenderAndShow(tx, Env.Root, 1);
				Env.Root.Add(tx, new string('6', 1300), new MemoryStream(new byte[2]));
				RenderAndShow(tx, Env.Root, 1);
				Env.Root.Add(tx, new string('0', 1300), new MemoryStream(new byte[4]));
				RenderAndShow(tx, Env.Root, 1);
				Env.Root.Add(tx, new string('3', 1000), new MemoryStream(new byte[1]));
				RenderAndShow(tx, Env.Root, 1);
				Env.Root.Add(tx, new string('7', 1300), new MemoryStream(new byte[1]));
				
				tx.Commit();

				RenderAndShow(tx, Env.Root, 1);
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.Root.Delete(tx, new string('0', 1300));

				tx.Commit();

				RenderAndShow(tx, Env.Root, 1);
			}
		}
	}
}